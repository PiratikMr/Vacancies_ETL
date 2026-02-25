package org.example.core.normalization.impl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.database.{DimCountryDef, DimLocationDef, MappingCountryDef, MappingLocationDef}
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.example.core.etl.model.{Vacancy, VacancyColumns}
import org.example.core.normalization.model.{NormCandidate, NormMatch, NormalizationColumns}
import org.example.core.normalization.service.NormalizeService

class HierarchicalNormalizer(spark: SparkSession,
                             dbAdapter: DataBaseAdapter,
                             childSettings: FuzzyMatchSettings,
                             parentSettings: FuzzyMatchSettings) {

  import spark.implicits._

  def normalize(vacancies: Dataset[Vacancy]): Dataset[NormMatch] = {

    val rawData = vacancies.toDF()
      .select(
        col(VacancyColumns.EXTERNAL_ID),
        explode_outer(col(VacancyColumns.LOCATIONS)).as("loc_struct")
      )
      .filter(col("loc_struct").isNotNull)
      .withColumn("uniqueId", monotonically_increasing_id().cast("string"))
      .cache()

    // 1. Страны
    val countriesData = rawData.select(
      col("uniqueId").as(NormalizationColumns.ENTITY_ID),
      col("loc_struct").getField(VacancyColumns.COUNTRY).cast("string").as(NormalizationColumns.RAW_VALUE),
      lit(null).cast("string").as(NormalizationColumns.PARENT_ID)
    ).filter(col(NormalizationColumns.RAW_VALUE).isNotNull).as[NormCandidate]

    val normalizedCountries = countryNormalizeService.mapSimple(countriesData, withCreate = true)

    // 2. Локации
    val locationsData = rawData
      .join(normalizedCountries.toDF(), col("uniqueId") === col(NormalizationColumns.ENTITY_ID), "inner")
      .select(
        col(VacancyColumns.EXTERNAL_ID).as(NormalizationColumns.ENTITY_ID), // Сразу возвращаем ID вакансии!
        col("loc_struct").getField(VacancyColumns.LOCATION).cast("string").as(NormalizationColumns.RAW_VALUE),
        col(NormalizationColumns.MAPPED_ID).as(NormalizationColumns.PARENT_ID) // ID страны как parent
      ).filter(col(NormalizationColumns.RAW_VALUE).isNotNull).as[NormCandidate]

    val normalizedLocations = locationNormalizeService.mapSimple(locationsData, withCreate = true)

    rawData.unpersist(blocking = false)
    normalizedLocations
  }

  private val countryNormalizeService = new NormalizeService(spark, dbAdapter, parentSettings, DimCountryDef, MappingCountryDef)
  private val locationNormalizeService = new NormalizeService(spark, dbAdapter, childSettings, DimLocationDef, MappingLocationDef)
}