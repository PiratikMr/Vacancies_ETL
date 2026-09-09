package org.example.core.normalization.impl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.database.{DimCountryDef, DimLocationDef, MappingCountryDef, MappingLocationDef}
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.example.core.etl.model.{Vacancy, VacancyColumns}
import org.example.core.normalization.api.BaseNormalizer
import org.example.core.normalization.impl.HierarchicalNormalizer._
import org.example.core.normalization.model.NormalizationColumns._
import org.example.core.normalization.model.NormalizersEnum.LOCATIONS
import org.example.core.normalization.model.{MatchLogPart, NormCandidate, NormalizationOutput}
import org.example.core.normalization.service.NormalizeService
import org.example.core.util.CheckpointSupport._

class HierarchicalNormalizer(spark: SparkSession,
                             dbAdapter: DataBaseAdapter,
                             childSettings: FuzzyMatchSettings,
                             parentSettings: FuzzyMatchSettings) extends BaseNormalizer {

  import spark.implicits._

  private val countryNormalizeService = new NormalizeService(spark, dbAdapter, parentSettings, DimCountryDef, MappingCountryDef)
  private val locationNormalizeService = new NormalizeService(spark, dbAdapter, childSettings, DimLocationDef, MappingLocationDef)

  override def process(vacancies: Dataset[Vacancy], withCreate: Boolean): NormalizationOutput = {
    val rawData = vacancies.toDF()
      .select(
        col(VacancyColumns.EXTERNAL_ID),
        posexplode_outer(col(VacancyColumns.LOCATIONS)).as(Seq(POS, LOC_STRUCT))
      )
      .filter(col(LOC_STRUCT).isNotNull)
      .withColumn(UNIQUE_ID, concat(col(VacancyColumns.EXTERNAL_ID), lit("_"), col(POS)))
      .cache()

    val countriesData = rawData.select(
      col(UNIQUE_ID).as(ENTITY_ID),
      col(LOC_STRUCT).getField(VacancyColumns.COUNTRY).cast("string").as(RAW_VALUE),
      lit(null).cast("string").as(PARENT_ID)
    ).filter(col(RAW_VALUE).isNotNull).as[NormCandidate]

    val normalizedCountries = countryNormalizeService.mapSimple(countriesData, withCreate)

    val locationsData = rawData
      .join(normalizedCountries.matches.toDF(), col(UNIQUE_ID) === col(ENTITY_ID), "inner")
      .select(
        col(VacancyColumns.EXTERNAL_ID).as(ENTITY_ID), // Сразу берем внешний ID
        col(LOC_STRUCT).getField(VacancyColumns.LOCATION).cast("string").as(RAW_VALUE),
        col(MAPPED_ID).cast("string").as(PARENT_ID) // ID страны
      ).filter(col(RAW_VALUE).isNotNull).as[NormCandidate]

    val normalizedLocations = locationNormalizeService.mapSimple(locationsData, withCreate)

    val countryLog = normalizedCountries.log.toDF()
      .join(rawData.select(col(UNIQUE_ID), col(VacancyColumns.EXTERNAL_ID)), col(ENTITY_ID) === col(UNIQUE_ID))
      .select(
        col(VacancyColumns.EXTERNAL_ID).as(ENTITY_ID),
        col(MAPPING_ID),
        col(RAW_VALUE),
        col(SCORE)
      )
      .distinct()
      .reliableCheckpoint()

    val mappings = normalizedLocations.matches.toDF()
      .groupBy(ENTITY_ID)
      .agg(collect_list(MAPPED_ID).as(LOCATIONS.mappedIdCol))

    rawData.unpersist(blocking = false)

    NormalizationOutput(
      mappings = mappings,
      matchLogs = Seq(
        MatchLogPart(MappingCountryDef.matchLogDef, countryLog),
        MatchLogPart(MappingLocationDef.matchLogDef, normalizedLocations.log.toDF())
      )
    )
  }
}

object HierarchicalNormalizer {
  private val POS = "pos"
  private val LOC_STRUCT = "loc_struct"
  private val UNIQUE_ID = "uniqueId"
}
