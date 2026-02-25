package org.example.core.normalization.factory

import org.apache.spark.sql.SparkSession
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.database._
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.example.core.normalization.impl.{HierarchicalNormalizer, LanguageNormalizer}
import org.example.core.normalization.model.NormalizersEnum._
import org.example.core.normalization.service.NormalizeService

object NormalizerFactory {

  // Теперь возвращает просто NormalizeService!
  def getNormalizeService(spark: SparkSession,
                          dbAdapter: DataBaseAdapter,
                          settings: FuzzyMatchSettings,
                          normalizerType: GroupNonHierarchical
                         ): NormalizeService = {
    normalizerType match {
      case CURRENCY => new NormalizeService(spark, dbAdapter, settings, DimCurrencyDef, MappingCurrencyDef)
      case EMPLOYER => new NormalizeService(spark, dbAdapter, settings, DimEmployerDef, MappingEmployerDef)
      case EXPERIENCE => new NormalizeService(spark, dbAdapter, settings, DimExperienceDef, MappingExperienceDef)
      case PLATFORM => new NormalizeService(spark, dbAdapter, settings, DimPlatformDef, MappingPlatformDef)
      case EMPLOYMENTS => new NormalizeService(spark, dbAdapter, settings, DimEmploymentDef, MappingEmploymentDef)
      case FIELDS => new NormalizeService(spark, dbAdapter, settings, DimFieldDef, MappingFieldDef)
      case GRADES => new NormalizeService(spark, dbAdapter, settings, DimGradeDef, MappingGradeDef)
      case SCHEDULES => new NormalizeService(spark, dbAdapter, settings, DimScheduleDef, MappingScheduleDef)
      case SKILLS => new NormalizeService(spark, dbAdapter, settings, DimSkillDef, MappingSkillDef)
      case COUNTRY => new NormalizeService(spark, dbAdapter, settings, DimCountryDef, MappingCountryDef)
      case LANGUAGES | LANGUAGES_LEVEL => throw new IllegalArgumentException("Для языков используйте getLanguageNormalizer")
    }
  }

  def getLocationsNormalizer(spark: SparkSession, dbAdapter: DataBaseAdapter,
                             locationSettings: FuzzyMatchSettings, countrySettings: FuzzyMatchSettings): HierarchicalNormalizer = {
    new HierarchicalNormalizer(spark, dbAdapter, locationSettings, countrySettings)
  }

  def getLanguageNormalizer(spark: SparkSession, dbAdapter: DataBaseAdapter,
                            levelSettings: FuzzyMatchSettings, languageSettings: FuzzyMatchSettings): LanguageNormalizer = {
    new LanguageNormalizer(spark, dbAdapter, languageSettings, levelSettings)
  }
}