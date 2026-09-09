package org.example.core.normalization.service

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.database.{DimDef, MappingDimDef, MappingOrigin}
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.example.core.normalization.engine.FuzzyMatcher
import org.example.core.normalization.engine.model.{FuzzyCandidate, FuzzyColumns, FuzzyDictionary, FuzzyMappingMeta, FuzzyMatch}
import org.example.core.normalization.model._
import org.example.core.normalization.service.NormalizeService._
import org.example.core.util.CheckpointSupport._

class NormalizeService(
                        spark: SparkSession,
                        dbAdapter: DataBaseAdapter,
                        settings: FuzzyMatchSettings,
                        dimDef: DimDef,
                        mappingDef: MappingDimDef,
                        fuzzyMatcherOpt: Option[FuzzyMatcher] = None
                      ) {

  import spark.implicits._

  private val fuzzyMatcher = fuzzyMatcherOpt.getOrElse(FuzzyMatcher(spark, settings))


  // id, value, parent_id
  private def saveDimTable(df: DataFrame, valueCol: String): DataFrame = {

    val colsToWrite = Seq(col(valueCol).as(dimDef.entityName)) ++
      dimDef.parentId.map(name => col(parentId).as(name))

    val returnCols = Seq(dimDef.entityId, dimDef.entityName) ++ dimDef.parentId.toSeq

    val saved = dbAdapter.saveWithReturn(
      spark = spark,
      df = df.select(colsToWrite: _*),
      targetTable = dimDef.meta.tableName,
      returns = returnCols,
      conflicts = dimDef.meta.conflictKeys
    )

    val res = saved
      .withColumnRenamed(dimDef.entityId, mappedId)
      .withColumnRenamed(dimDef.entityName, valueCol)

    dimDef.parentId match {
      case Some(name) => res.withColumnRenamed(name, parentId)
      case None => res.withColumn(parentId, lit(DEFAULT_PARENT_ID))
    }
  }


  private def saveMappingTable(mappingData: Dataset[FuzzyMappingMeta], reloadedDims: DataFrame): DataFrame = {

    val toWrite = mappingData.toDF().alias("map")
      .join(reloadedDims.alias("dim"),
        col(s"map.${FuzzyColumns.HUB_VALUE}") === col(s"dim.${NormalizationColumns.RAW_VALUE}") &&
          col(s"map.${FuzzyColumns.PARENT_ID}") === col(s"dim.$parentId")
      )
      .select(
        col(s"dim.$mappedId").as(mappingDef.entityId),
        col(s"map.${FuzzyColumns.NORM_VALUE}").as(mappingDef.mappedValue),
        col(s"map.${FuzzyColumns.IS_CANONICAL}").as(mappingDef.isCanonical),
        col(s"map.${FuzzyColumns.LINK_SCORE}").as(mappingDef.linkScore),
        lit(MappingOrigin.ETL).as(mappingDef.origin)
      )

    dbAdapter.saveWithReturn(
      spark = spark,
      df = toWrite,
      targetTable = mappingDef.meta.tableName,
      returns = Seq(mappingDef.mappingId, mappingDef.entityId, mappingDef.mappedValue),
      conflicts = mappingDef.meta.conflictKeys
    )
      .withColumnRenamed(mappingDef.mappingId, mappingIdCol)
      .withColumnRenamed(mappingDef.entityId, mappedId)
      .withColumnRenamed(mappingDef.mappedValue, normValue)
  }


  // [id, mapping_id, norm_value, is_canonical, parent_id]
  private def loadFullMappingTable(): DataFrame = {

    val parentSelect = dimDef.parentId.map(n => s"d.$n").getOrElse(DEFAULT_PARENT_ID.toString)

    val query =
      s"""
         |SELECT  d.${dimDef.entityId} as $mappedId,
         |        md.${mappingDef.mappingId} as $mappingIdCol,
         |        md.${mappingDef.mappedValue} as $normValue,
         |        md.${mappingDef.isCanonical} as $isCanonical,
         |        $parentSelect as $parentId
         |FROM ${mappingDef.meta.tableName} as md
         |JOIN ${dimDef.meta.tableName} as d on d.${dimDef.entityId} = md.${mappingDef.entityId}
         |WHERE md.${mappingDef.isActive}
         |""".stripMargin

    dbAdapter.loadQuery(spark, query)
  }


  private def buildDictionary(fullMappingTable: DataFrame): Dataset[FuzzyDictionary] = {
    fullMappingTable
      .select(
        col(mappedId).as(FuzzyColumns.DICT_ID),
        col(mappingIdCol).as(FuzzyColumns.MAPPING_ID),
        col(normValue).as(FuzzyColumns.NORM_VALUE),
        col(parentId).as(FuzzyColumns.PARENT_ID)
      ).as[FuzzyDictionary]
  }


  private def buildCandidates(candidates: Dataset[NormCandidate]): Dataset[FuzzyCandidate] = {
    candidates.toDF()
      .withColumn(FuzzyColumns.PARENT_ID, coalesce(col(NormalizationColumns.PARENT_ID), lit(DEFAULT_PARENT_ID)))
      .select(
        col(NormalizationColumns.ENTITY_ID).as(FuzzyColumns.ENTITY_ID),
        col(NormalizationColumns.RAW_VALUE).as(FuzzyColumns.RAW_VALUE),
        col(FuzzyColumns.PARENT_ID)
      )
      .as[FuzzyCandidate]
  }


  private def buildResult(matches: Dataset[FuzzyMatch]): NormalizeResult = {

    val checkpointed = matches.reliableCheckpoint().toDF()

    val matchesDs = checkpointed
      .select(
        col(FuzzyColumns.ENTITY_ID).as(NormalizationColumns.ENTITY_ID),
        col(FuzzyColumns.DICT_ID).as(NormalizationColumns.MAPPED_ID)
      )
      .distinct()
      .as[NormMatch]

    val logDs = checkpointed
      .select(
        col(FuzzyColumns.ENTITY_ID).as(NormalizationColumns.ENTITY_ID),
        col(FuzzyColumns.MAPPING_ID).as(NormalizationColumns.MAPPING_ID),
        col(FuzzyColumns.RAW_VALUE).as(NormalizationColumns.RAW_VALUE),
        col(FuzzyColumns.SCORE).as(NormalizationColumns.SCORE)
      )
      .distinct()
      .as[MatchLogRow]

    NormalizeResult(matchesDs, logDs)
  }


  private def emptyResult: NormalizeResult =
    NormalizeResult(spark.emptyDataset[NormMatch], spark.emptyDataset[MatchLogRow])


  def extractTags(candidates: Dataset[NormCandidate]): NormalizeResult = {

    val fullMappingTable = loadFullMappingTable().cache() // [id, mapping_id, norm_value, is_canonical, parent_id]

    if (fullMappingTable.isEmpty) {
      fullMappingTable.unpersist(blocking = false)
      return emptyResult
    }

    val exactMatchesDs = fuzzyMatcher.extractTags(buildCandidates(candidates), buildDictionary(fullMappingTable))

    val res = buildResult(exactMatchesDs)

    fullMappingTable.unpersist(blocking = false)

    res
  }


  def mapSimple(candidates: Dataset[NormCandidate], withCreate: Boolean): NormalizeResult = {

    val fullMappingTable = loadFullMappingTable().cache() // [id, mapping_id, norm_value, is_canonical, parent_id]

    val fuzzyRes = fuzzyMatcher.execute(
      candidatesDs = buildCandidates(candidates),
      dictionaryDs = buildDictionary(fullMappingTable)
    )


    if (!withCreate || fuzzyRes.toCreate.isEmpty) {
      val res = buildResult(fuzzyRes.matched)
      fuzzyRes.clearCache()
      fullMappingTable.unpersist(blocking = false)
      return res
    }


    val toCreateDf = fuzzyRes.toCreate.toDF().cache()

    val newDimsToWrite = toCreateDf
      .select(
        col(FuzzyColumns.HUB_VALUE).as(NormalizationColumns.RAW_VALUE),
        col(FuzzyColumns.PARENT_ID).as(parentId)
      )
      .distinct()

    val reloadedDims = saveDimTable(newDimsToWrite, NormalizationColumns.RAW_VALUE).cache()
    val savedMappings = saveMappingTable(fuzzyRes.mappingData, reloadedDims).cache()

    val createdMatches = toCreateDf.alias("create")
      .join(reloadedDims.alias("dim"),
        col(s"create.${FuzzyColumns.HUB_VALUE}") === col(s"dim.${NormalizationColumns.RAW_VALUE}") &&
          col(s"create.${FuzzyColumns.PARENT_ID}") === col(s"dim.$parentId")
      )
      .join(savedMappings.alias("map"),
        col(s"dim.$mappedId") === col(s"map.$mappedId") &&
          col(s"create.${FuzzyColumns.NORM_VALUE}") === col(s"map.$normValue")
      )
      .select(
        col(s"create.${FuzzyColumns.ENTITY_ID}").as(FuzzyColumns.ENTITY_ID),
        col(s"dim.$mappedId").as(FuzzyColumns.DICT_ID),
        col(s"map.$mappingIdCol").as(FuzzyColumns.MAPPING_ID),
        col(s"create.${FuzzyColumns.RAW_VALUE}").as(FuzzyColumns.RAW_VALUE),
        col(s"create.${FuzzyColumns.SCORE}").as(FuzzyColumns.SCORE)
      )

    val allMatches = fuzzyRes.matched.toDF()
      .unionByName(createdMatches)
      .as[FuzzyMatch]

    val res = buildResult(allMatches)

    fuzzyRes.clearCache()
    fullMappingTable.unpersist(blocking = false)
    toCreateDf.unpersist(blocking = false)
    reloadedDims.unpersist(blocking = false)
    savedMappings.unpersist(blocking = false)

    res
  }

}

object NormalizeService {
  private val mappedId = "id"
  private val mappingIdCol = "mapping_id"
  private val normValue = "norm_value"
  private val isCanonical = "is_origin"
  private val parentId = "parent_id"
  private val DEFAULT_PARENT_ID = -1L
}
