package org.example.core.normalization.service

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.example.core.normalization.config.{DimTableConf, MappingDimTableConf}
import org.example.core.normalization.model.{FuzzyCandidate, FuzzyDictionary, NormalizeServiceResult}
import org.example.core.normalization.service.NormalizeService._
import org.example.core.normalization.service.matching.FuzzyMatcher

class NormalizeService(
                        spark: SparkSession,
                        dbAdapter: DataBaseAdapter,
                        settings: FuzzyMatchSettings,
                        dt: DimTableConf,
                        mdt: MappingDimTableConf
                      ) {

  import spark.implicits._

  private val fuzzyMatcher = FuzzyMatcher(
    spark = spark,
    settings = settings
  )


  // id, value, parent_id
  private def saveDimTable(
                            df: DataFrame,
                            valueCol: String
                          ): DataFrame = {

    val colsToWrite = Seq(col(valueCol).as(dt.valueColName)) ++
      dt.parentIdColName.map(name => col(parentId).as(name))

    val returnCols = Seq(dt.idColName, dt.valueColName) ++
      dt.parentIdColName.toSeq
    val conflictCols = Seq(dt.valueColName) ++ dt.parentIdColName.toSeq

    val saved = dbAdapter.saveWithReturn(
      spark = spark,
      df = df.select(colsToWrite: _*),
      targetTable = dt.tableName,
      returns = returnCols,
      conflicts = conflictCols
    )

    val res = saved
      .withColumnRenamed(dt.idColName, mappedId)
      .withColumnRenamed(dt.valueColName, valueCol)

    dt.parentIdColName match {
      case Some(name) => res.withColumnRenamed(name, parentId)
      case None => res.withColumn(parentId, lit(DEFAULT_PARENT_ID))
    }
  }


  // [id, norm_value, is_canonical, parent_id]
  private def loadFullMappingTable(): DataFrame = {

    val parentSelect = dt.parentIdColName.map(n => s"d.$n").getOrElse(DEFAULT_PARENT_ID.toString)

    val query =
      s"""
         |SELECT  d.${mdt.idColName} as $mappedId,
         |        md.${mdt.mappedValueColName} as $normValue,
         |        md.${mdt.isOrigin} as $isCanonical,
         |        $parentSelect as $parentId
         |FROM ${mdt.tableName} as md
         |JOIN ${dt.tableName} as d on d.${dt.idColName} = md.${mdt.idColName}
         |""".stripMargin

    dbAdapter.loadQuery(spark, query)
  }


  // [vacancy_id, id]
  def extractTags(candidates: DataFrame, // [vacancy_id, value]
                  entityIdCol: String,
                  valueCol: String
                 ): NormalizeServiceResult = {

    val fullMappingTable = loadFullMappingTable().cache() // [id, norm_value, is_canonical, parent_id]

    if (fullMappingTable.isEmpty) {
      return returnResult(spark.emptyDataFrame)
    }

    val maxN = fullMappingTable
      .select(size(split(col(normValue), " ")).as("len"))
      .agg(max("len"))
      .head()
      .getInt(0)


    val uniGramsExpr = makeSortedNGrams(col(normValue), 1)

    val allGramsExpr = (2 to maxN)
      .foldLeft(uniGramsExpr)((res, i) =>
        concat(res, makeSortedNGrams(col(normValue), i))
      )

    val finalRes = candidates
      .withColumn(normValue, fuzzyMatcher.normArrayCol(col(valueCol)))
      .withColumn("c", allGramsExpr)
      .select(col(entityIdCol), explode(col("c")).as(normValue))
      .distinct()

      .join(fullMappingTable, Seq(normValue))
      .select(entityIdCol, mappedId)
      .distinct()

    returnResult(finalRes)
  }


  // [vacancy_id, (value), id]
  def mapSimple(candidates: DataFrame, // [vacancy_id, value, (parent_id)]
                entityIdCol: String,
                valueCol: String,
                parentIdCol: Option[String],
                withCreate: Boolean
               ): NormalizeServiceResult = {

    val fullMappingTable = loadFullMappingTable().cache() // [id, norm_value, is_canonical, parent_id]

    val dictionaryDs = fullMappingTable
      .select(
        col(mappedId).as("id"),
        col(normValue).as("normValue"),
        col(parentId).as("parentId")
      )
      .as[FuzzyDictionary]

    val rawCandidates = candidates
      .withColumn(parentId, parentIdCol.map(col).getOrElse(lit(DEFAULT_PARENT_ID)))
      .select(
        col(entityIdCol).as("entityId"),
        col(valueCol).as("rawValue"),
        col(parentId).as("parentId")
      )
      .as[FuzzyCandidate]

    val fuzzyRes = fuzzyMatcher.execute(
      candidatesDs = rawCandidates,
      dictionaryDs = dictionaryDs
    )


    val matchedResult = fuzzyRes.matched.toDF()
      .select(
        col("entityId").as(entityIdCol),
        col("dictId").as(mappedId)
      )

    if (!withCreate || fuzzyRes.toCreate.isEmpty) {
      val checkPointedRes = matchedResult.localCheckpoint()
      fuzzyRes.clearCache()
      return returnResult(checkPointedRes)
    }


    val newDimsToWrite = fuzzyRes.toCreate.toDF()
      .select(
        col("rawValue").as(valueCol),
        col("parentId").as(parentId)
      )

    val reloadedDims = saveDimTable(newDimsToWrite, valueCol).cache()

    // 2. Получаем маппинг "Кандидат -> Новый ID"
    val createdMapping = fuzzyRes.toCreate.toDF()
      .join(reloadedDims,
        col("rawValue") === col(valueCol) &&
          col("parentId") === col(parentId)
      )
      .select(
        col("entityId").as(entityIdCol),
        col(mappedId)
      )

    // 3. Сохраняем метаданные матчинга в Mapping Table
    if (!fuzzyRes.mappingData.isEmpty) {
      val mappingDataToWrite = fuzzyRes.mappingData.toDF()
        .join(reloadedDims,
          col("rawValue") === col(valueCol) &&
            col("parentId") === col(parentId)
        )
        .select(
          col("normValue").as(mdt.mappedValueColName),
          col("isCanonical").as(mdt.isOrigin),
          col(mappedId).as(mdt.idColName)
        )

      dbAdapter.save(mappingDataToWrite, mdt.tableName, Seq(mdt.mappedValueColName, mdt.idColName))
    }

    val finalRes = matchedResult.union(createdMapping).distinct()

    val checkPointedRes = finalRes.localCheckpoint()
    fuzzyRes.clearCache()

    returnResult(checkPointedRes)
  }


  private def makeSortedNGrams(arrC: Column, n: Int): Column = {
    val arrLen = size(arrC)

    val indices = sequence(lit(1), arrLen - lit(n) + lit(1))

    when(arrLen >= lit(n),
      transform(indices, i => {
        val chunk = slice(arrC, i, lit(n))
        array_join(array_sort(chunk), " ")
      })
    ).otherwise(array().cast("array<string>"))
  }

}

object NormalizeService {
  private val mappedId = "id"
  private val normValue = "norm_value"
  private val isCanonical = "is_origin"
  private val parentId = "parent_id"
  private val DEFAULT_PARENT_ID = -1

  private def returnResult(df: DataFrame): NormalizeServiceResult = {
    NormalizeServiceResult(df, mappedId)
  }
}