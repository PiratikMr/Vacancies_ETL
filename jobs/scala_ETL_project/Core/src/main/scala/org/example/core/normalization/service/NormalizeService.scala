package org.example.core.normalization.service

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.example.core.normalization.model.{DimTableConf, MappingDimTableConf, NormalizeServiceResult}
import org.example.core.normalization.service.NormalizeService._
import org.example.core.normalization.service.matching.FuzzyMatcher

class NormalizeService(
                        spark: SparkSession,
                        dbAdapter: DataBaseAdapter,
                        settings: FuzzyMatchSettings,
                        dt: DimTableConf,
                        mdt: MappingDimTableConf
                      ) {

  private val fuzzyMatcher = new FuzzyMatcher(
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

    val inputClean = candidates
      .withColumn(parentId, parentIdCol.map(col).getOrElse(lit(DEFAULT_PARENT_ID)))
      .withColumn(normValue, fuzzyMatcher.normCol(col(valueCol)))
      .cache()

    val joinKeys = Seq(normValue, parentId)

    val resolvedFromMap = inputClean.join(fullMappingTable, joinKeys) // [vacancy_id, id, norm_value, is_canonical, parent_id]

    val noInMappingTable = inputClean.join(resolvedFromMap, joinKeys :+ entityIdCol, "left_anti") // [vacancy_id, value, norm_value, parent_id]


    if (noInMappingTable.isEmpty || !withCreate) {
      return returnResult(resolvedFromMap.select(entityIdCol, valueCol, mappedId))
    }


    val canonicalDict = fullMappingTable.filter(col(isCanonical)).select(mappedId, normValue, parentId)


    val newMappings = processNewValues(
      noInMappingTable,
      canonicalDict,
      entityIdCol,
      valueCol
    ) // [vacancy_id, id]

    val finalRes = resolvedFromMap
      .select(entityIdCol, mappedId)
      .union(newMappings)
      .distinct()

    returnResult(finalRes)
  }

  // [vacancy_id, id]
  private def processNewValues(noInMappingTable: DataFrame, // [vacancy_id, value, norm_value, parent_id]
                               dimTable: DataFrame, // [id, norm_value, parent_id],
                               entityIdCol: String,
                               valueCol: String
                              ): DataFrame = {


    val fuzzyRes = fuzzyMatcher.execute(
      candidatesDf = noInMappingTable,
      dictDf = dimTable,
      cEntityId = entityIdCol,
      cRawValue = valueCol,
      cNormValue = normValue,
      cParentId = parentId,
      dId = mappedId,
      dNormValue = normValue,
      dParentId = parentId
    )

    if (fuzzyRes.matchedDf.isEmpty) {
      val mappingsToSave = fuzzyRes.matchedDf
        .select(
          col(normValue).as(mdt.mappedValueColName),
          col(mappedId).as(mdt.idColName)
        )
        .distinct()
        .withColumn(mdt.isOrigin, lit(false))

      dbAdapter.save(mappingsToSave, mdt.tableName, Seq(mdt.idColName, mdt.mappedValueColName))
    }

    val dictMatchedResult = fuzzyRes.matchedDf
      .select(entityIdCol, mappedId)


    if (fuzzyRes.toCreateDf.isEmpty) {
      return dictMatchedResult
    }


    val newDimsToWrite = fuzzyRes.toCreateDf
      .select(valueCol, parentId)
      .distinct()

    val reloadedDims = saveDimTable(newDimsToWrite, valueCol)
      .cache()

    val createdMapping = fuzzyRes.toCreateDf
      .join(reloadedDims, Seq(valueCol, parentId))
      .select(col(entityIdCol), col(mappedId))

    if (!fuzzyRes.mappingDataDf.isEmpty) {
      val mappingDataToWrite = fuzzyRes.mappingDataDf
        .join(reloadedDims, Seq(valueCol, parentId))
        .select(
          col(normValue).as(mdt.mappedValueColName),
          col(fuzzyRes.isCanonicalCol).as(mdt.isOrigin),
          col(mappedId).as(mdt.idColName)
        )

      dbAdapter.save(mappingDataToWrite, mdt.tableName, Seq(mdt.mappedValueColName, mdt.idColName))
    }

    dictMatchedResult.union(createdMapping).distinct()
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