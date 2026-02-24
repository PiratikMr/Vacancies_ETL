package org.example.core.normalization.factory

import org.apache.spark.sql.SparkSession
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.database._
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.example.core.etl.model.VacancyColumns
import org.example.core.etl.model.VacancyColumns.EXTERNAL_ID
import org.example.core.normalization.api.Normalizer
import org.example.core.normalization.impl.strategy.{Aggregators, Extractors}
import org.example.core.normalization.impl.{GenericNormalizer, HierarchicalNormalizer, LanguageNormalizer}
import org.example.core.normalization.model.NormalizersEnum._
import org.example.core.normalization.service.NormalizeService

object NormalizerFactory {

  def getNonHierarchicalNormalizer(spark: SparkSession,
                                   dbAdapter: DataBaseAdapter,
                                   settings: FuzzyMatchSettings,
                                   normalizerType: GroupNonHierarchical
                                  ): Normalizer = {
    normalizerType match {
      case CURRENCY => getSimpleNormalizer(spark, dbAdapter, settings, VacancyColumns.CURRENCY, DimCurrencyDef, MappingCurrencyDef)
      case EMPLOYER => getSimpleNormalizer(spark, dbAdapter, settings, VacancyColumns.EMPLOYER, DimEmployerDef, MappingEmployerDef)
      case EXPERIENCE => getSimpleNormalizer(spark, dbAdapter, settings, VacancyColumns.EXPERIENCE, DimExperienceDef, MappingExperienceDef)
      case PLATFORM => getSimpleNormalizer(spark, dbAdapter, settings, VacancyColumns.PLATFORM, DimPlatformDef, MappingPlatformDef)

      case EMPLOYMENTS => getArrayNormalizer(spark, dbAdapter, settings, VacancyColumns.EMPLOYMENTS, DimEmploymentDef, MappingEmploymentDef)
      case FIELDS => getArrayNormalizer(spark, dbAdapter, settings, VacancyColumns.FIELDS, DimFieldDef, MappingFieldDef)
      case GRADES => getArrayNormalizer(spark, dbAdapter, settings, VacancyColumns.GRADES, DimGradeDef, MappingGradeDef)
      case SCHEDULES => getArrayNormalizer(spark, dbAdapter, settings, VacancyColumns.SCHEDULES, DimScheduleDef, MappingScheduleDef)
      case SKILLS => getArrayNormalizer(spark, dbAdapter, settings, VacancyColumns.SKILLS, DimSkillDef, MappingSkillDef)

      case _ => throw new IllegalArgumentException(s"Неподдерживаемый нормализатор: $normalizerType")
    }
  }

  def getLocationsNormalizer(spark: SparkSession, dbAdapter: DataBaseAdapter,
                             locationSettings: FuzzyMatchSettings, countrySettings: FuzzyMatchSettings): Normalizer = {
    new HierarchicalNormalizer(
      spark = spark, dbAdapter = dbAdapter,
      childSettings = locationSettings, parentSettings = countrySettings,
      entityIdCol = EXTERNAL_ID, arrayCol = VacancyColumns.LOCATIONS,
      childCol = VacancyColumns.LOCATION, parentCol = VacancyColumns.COUNTRY
    )
  }

  def getLanguageNormalizer(spark: SparkSession, dbAdapter: DataBaseAdapter,
                            levelSettings: FuzzyMatchSettings, languageSettings: FuzzyMatchSettings): Normalizer = {
    new LanguageNormalizer(
      spark = spark, dbAdapter = dbAdapter,
      levelSettings = levelSettings, languageSettings = languageSettings,
      entityIdCol = EXTERNAL_ID, arrayCol = VacancyColumns.LANGUAGES,
      levelCol = VacancyColumns.LEVEL, languageCol = VacancyColumns.LANGUAGE
    )
  }


  private def getSimpleNormalizer(spark: SparkSession, dbAdapter: DataBaseAdapter, settings: FuzzyMatchSettings,
                                  valueCol: String, dimDef: DimDef, mappingDef: MappingDimDef): Normalizer = {
    val ns = new NormalizeService(spark, dbAdapter, settings, dimDef, mappingDef)
    new GenericNormalizer(ns, Extractors.SimpleExtractor, Aggregators.NoAggregation, EXTERNAL_ID, valueCol)
  }

  private def getArrayNormalizer(spark: SparkSession, dbAdapter: DataBaseAdapter, settings: FuzzyMatchSettings,
                                 valueCol: String, dimDef: DimDef, mappingDef: MappingDimDef): Normalizer = {
    val ns = new NormalizeService(spark, dbAdapter, settings, dimDef, mappingDef)
    new GenericNormalizer(ns, Extractors.ArrayExtractor, Aggregators.ArrayAggregator, EXTERNAL_ID, valueCol)
  }
}
