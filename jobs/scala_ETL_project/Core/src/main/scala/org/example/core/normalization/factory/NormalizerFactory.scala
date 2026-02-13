package org.example.core.normalization.factory

import org.apache.spark.sql.SparkSession
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.example.core.config.schema.DataBaseOneToManyEntity
import org.example.core.config.schema.SchemaRegistry.DataBase.Entities
import org.example.core.config.schema.SchemaRegistry.Internal.RawVacancy
import org.example.core.normalization.api.Normalizer
import org.example.core.normalization.config.{DimTableConf, MappingDimTableConf}
import org.example.core.normalization.impl.strategy.{Aggregators, Extractors, RawDataExtractor, ResultAggregator}
import org.example.core.normalization.impl.{GenericNormalizer, HierarchicalNormalizer, LanguageNormalizer}
import org.example.core.normalization.service.NormalizeService
import org.example.core.objects.NormalizersEnum._

object NormalizerFactory {

  def getNonHierarchicalNormalizer(spark: SparkSession,
                                   dbAdapter: DataBaseAdapter,
                                   settings: FuzzyMatchSettings,
                                   normalizerType: GroupNonHierarchical
                                  ): Normalizer = {
    normalizerType match {
      case CURRENCY => getCurrencyNormalizer(spark, dbAdapter, settings)
      case EMPLOYER => getEmployerNormalizer(spark, dbAdapter, settings)
      case EXPERIENCE => getExperienceNormalizer(spark, dbAdapter, settings)
      case PLATFORM => getPlatformNormalizer(spark, dbAdapter, settings)
      case EMPLOYMENTS => getEmploymentsNormalizer(spark, dbAdapter, settings)
      case FIELDS => getFieldsNormalizer(spark, dbAdapter, settings)
      case GRADES => getGradesNormalizer(spark, dbAdapter, settings)
      case SCHEDULES => getSchedulesNormalizer(spark, dbAdapter, settings)
      case SKILLS => getSkillsNormalizer(spark, dbAdapter, settings)
      //      case LOCATIONS => getLocationsNormalizer(spark, dbAdapter, settings)
      //      case LANGUAGES => getCurrencyNormalizer(spark, dbAdapter, settings)
    }
  }


  def getCurrencyNormalizer(spark: SparkSession, dbAdapter: DataBaseAdapter, settings: FuzzyMatchSettings): Normalizer = {
    getSimpleNormalizer(spark, dbAdapter, settings, RawVacancy.currency.name, Entities.Currency)
  }

  def getEmployerNormalizer(spark: SparkSession, dbAdapter: DataBaseAdapter, settings: FuzzyMatchSettings): Normalizer = {
    getSimpleNormalizer(spark, dbAdapter, settings, RawVacancy.employer.name, Entities.Employer)
  }

  def getExperienceNormalizer(spark: SparkSession, dbAdapter: DataBaseAdapter, settings: FuzzyMatchSettings): Normalizer = {
    getSimpleNormalizer(spark, dbAdapter, settings, RawVacancy.experience.name, Entities.Experience)
  }

  def getPlatformNormalizer(spark: SparkSession, dbAdapter: DataBaseAdapter, settings: FuzzyMatchSettings): Normalizer = {
    getSimpleNormalizer(spark, dbAdapter, settings, RawVacancy.platform.name, Entities.Platform)
  }


  def getEmploymentsNormalizer(spark: SparkSession, dbAdapter: DataBaseAdapter, settings: FuzzyMatchSettings): Normalizer = {
    getArrayNormalizer(spark, dbAdapter, settings, RawVacancy.employments.name, Entities.Employments)
  }

  def getFieldsNormalizer(spark: SparkSession, dbAdapter: DataBaseAdapter, settings: FuzzyMatchSettings): Normalizer = {
    getArrayNormalizer(spark, dbAdapter, settings, RawVacancy.fields.name, Entities.Fields)
  }

  def getGradesNormalizer(spark: SparkSession, dbAdapter: DataBaseAdapter, settings: FuzzyMatchSettings): Normalizer = {
    getArrayNormalizer(spark, dbAdapter, settings, RawVacancy.grades.name, Entities.Grades)
  }

  def getSchedulesNormalizer(spark: SparkSession, dbAdapter: DataBaseAdapter, settings: FuzzyMatchSettings): Normalizer = {
    getArrayNormalizer(spark, dbAdapter, settings, RawVacancy.schedules.name, Entities.Schedules)
  }

  def getSkillsNormalizer(spark: SparkSession, dbAdapter: DataBaseAdapter, settings: FuzzyMatchSettings): Normalizer = {
    getArrayNormalizer(spark, dbAdapter, settings, RawVacancy.skills.name, Entities.Skills)
  }


  def getLocationsNormalizer(spark: SparkSession, dbAdapter: DataBaseAdapter,
                             locationSettings: FuzzyMatchSettings, countrySettings: FuzzyMatchSettings): Normalizer = {
    new HierarchicalNormalizer(
      spark = spark,
      dbAdapter = dbAdapter,
      childSettings = locationSettings,
      parentSettings = countrySettings,
      entityIdCol = RawVacancy.externalId.name,
      arrayCol = RawVacancy.locations.name,
      childCol = RawVacancy.locationRegion.name,
      parentCol = RawVacancy.locationCountry.name
    )
  }

  def getLanguageNormalizer(spark: SparkSession, dbAdapter: DataBaseAdapter,
                            levelSettings: FuzzyMatchSettings, languageSettings: FuzzyMatchSettings): Normalizer = {
    new LanguageNormalizer(
      spark = spark,
      dbAdapter = dbAdapter,
      levelSettings = levelSettings,
      languageSettings = languageSettings,
      entityIdCol = RawVacancy.externalId.name,
      arrayCol = RawVacancy.languages.name,
      levelCol = RawVacancy.languageLevel.name,
      languageCol = RawVacancy.languageLanguage.name
    )
  }


  private def getSimpleNormalizer(spark: SparkSession,
                                  dbAdapter: DataBaseAdapter,
                                  settings: FuzzyMatchSettings,
                                  valueCol: String,
                                  dbEntity: DataBaseOneToManyEntity
                                 ): Normalizer = {
    getNormalizer(spark, dbAdapter, settings, valueCol, dbEntity, Extractors.SimpleExtractor, Aggregators.NoAggregation)
  }


  private def getArrayNormalizer(spark: SparkSession,
                                 dbAdapter: DataBaseAdapter,
                                 settings: FuzzyMatchSettings,
                                 valueCol: String,
                                 dbEntity: DataBaseOneToManyEntity
                                ): Normalizer = {
    getNormalizer(spark, dbAdapter, settings, valueCol, dbEntity, Extractors.ArrayExtractor, Aggregators.ArrayAggregator)
  }


  private def getNormalizer(spark: SparkSession,
                            dbAdapter: DataBaseAdapter,
                            settings: FuzzyMatchSettings,
                            valueCol: String,
                            dbEntity: DataBaseOneToManyEntity,
                            extractor: RawDataExtractor,
                            aggregator: ResultAggregator
                           ): Normalizer = {

    val dt = dbEntity.dimTable
    val mdt = dbEntity.mappingDimTable

    val normalizeService = new NormalizeService(
      spark = spark,
      dbAdapter = dbAdapter,
      settings = settings,
      dt = DimTableConf(dt.tableName, dt.entityId.name, dt.name.name, None),
      mdt = MappingDimTableConf(mdt.tableName, mdt.entityId.name, mdt.mappedValue.name, mdt.isCanonical.name)
    )

    new GenericNormalizer(
      normalizeService = normalizeService,
      extractor = extractor,
      aggregator = aggregator,
      entityIdCol = RawVacancy.externalId.name,
      valCol = valueCol
    )
  }
}
