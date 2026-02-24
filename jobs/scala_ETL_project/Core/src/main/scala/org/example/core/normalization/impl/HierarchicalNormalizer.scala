package org.example.core.normalization.impl

import org.apache.spark.sql.functions.{col, collect_list, explode, monotonically_increasing_id}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.database.{DimCountryDef, DimLocationDef, MappingCountryDef, MappingLocationDef}
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.example.core.normalization.api.Normalizer
import org.example.core.normalization.model.NormalizeServiceResult
import org.example.core.normalization.service.NormalizeService

class HierarchicalNormalizer(spark: SparkSession,
                             dbAdapter: DataBaseAdapter,
                             childSettings: FuzzyMatchSettings,
                             parentSettings: FuzzyMatchSettings,
                             entityIdCol: String,
                             arrayCol: String,
                             childCol: String,
                             parentCol: String
                            ) extends Normalizer {

  override def extractTags(data: DataFrame, valueCol: String): NormalizeServiceResult = ???

  override def matchExactData(data: DataFrame): NormalizeServiceResult = ???

  override def normalize(data: DataFrame): NormalizeServiceResult = {

    val rawData = data.select(col(entityIdCol), explode(col(arrayCol)).as(arrayCol))
      .withColumn(uniqueId, monotonically_increasing_id())
      .cache()

    val countriesData = rawData.select(col(uniqueId), col(arrayCol).getField(parentCol).as(parentCol))
    val normalizedCountriesResult = countryNormalizeService.mapSimple(
      candidates = countriesData,
      entityIdCol = uniqueId,
      valueCol = parentCol,
      parentIdCol = None,
      withCreate = true
    )
    val normalizedCountries = normalizedCountriesResult.mappedDf.localCheckpoint()


    val locationsData = rawData
      .join(normalizedCountries, Seq(uniqueId))
      .select(
        col(entityIdCol),
        col(arrayCol).getField(childCol).as(childCol),
        col(normalizedCountriesResult.mappedIdCol).as(parentId)
      )
    val normalizedLocationsResult = locationNormalizeService.mapSimple(
      candidates = locationsData,
      entityIdCol = entityIdCol,
      valueCol = childCol,
      parentIdCol = Some(parentId),
      withCreate = true
    )

    val finalRes = normalizedLocationsResult.mappedDf
      .groupBy(entityIdCol)
      .agg(collect_list(normalizedLocationsResult.mappedIdCol).as(normalizedLocationsResult.mappedIdCol))

    NormalizeServiceResult(finalRes, normalizedLocationsResult.mappedIdCol)
  }

  private val uniqueId = "unique_id_312312"
  private val parentId = "parent_id_214122"


  private val countryNormalizeService = new NormalizeService(
    spark, dbAdapter, parentSettings, DimCountryDef, MappingCountryDef
  )

  private val locationNormalizeService = new NormalizeService(
    spark, dbAdapter, childSettings, DimLocationDef, MappingLocationDef
  )
}