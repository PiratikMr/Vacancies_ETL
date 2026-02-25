package org.example.core.normalization.impl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.database.{DimLanguageDef, DimLanguageLevelDef, MappingLanguageDef, MappingLanguageLevelDef}
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.example.core.etl.model.{Vacancy, VacancyColumns}
import org.example.core.normalization.impl.model.NormLanguageMatch
import org.example.core.normalization.model.{NormCandidate, NormalizationColumns}
import org.example.core.normalization.service.NormalizeService

class LanguageNormalizer(spark: SparkSession,
                         dbAdapter: DataBaseAdapter,
                         languageSettings: FuzzyMatchSettings,
                         levelSettings: FuzzyMatchSettings) {

  import spark.implicits._

  def normalize(vacancies: Dataset[Vacancy]): Dataset[NormLanguageMatch] = {

    val rawData = vacancies.toDF()
      .select(
        col(VacancyColumns.EXTERNAL_ID).as("vacancyId"),
        explode_outer(col(VacancyColumns.LANGUAGES)).as("lang_struct")
      )
      .filter(col("lang_struct").isNotNull)
      .withColumn("uniqueId", monotonically_increasing_id().cast("string"))
      .cache()

    // 1. Нормализуем уровни
    val levelsData = rawData.select(
      col("uniqueId").as(NormalizationColumns.ENTITY_ID),
      col("lang_struct").getField(VacancyColumns.LEVEL).cast("string").as(NormalizationColumns.RAW_VALUE),
      lit(null).cast("string").as(NormalizationColumns.PARENT_ID)
    ).filter(col(NormalizationColumns.RAW_VALUE).isNotNull).as[NormCandidate]

    val nLevels = levelNormalizeService.mapSimple(levelsData, withCreate = true)

    // 2. Нормализуем языки
    val langsData = rawData.select(
      col("uniqueId").as(NormalizationColumns.ENTITY_ID),
      col("lang_struct").getField(VacancyColumns.LANGUAGE).cast("string").as(NormalizationColumns.RAW_VALUE),
      lit(null).cast("string").as(NormalizationColumns.PARENT_ID)
    ).filter(col(NormalizationColumns.RAW_VALUE).isNotNull).as[NormCandidate]

    val nLangs = languageNormalizeService.mapSimple(langsData, withCreate = true)

    // 3. Собираем обратно
    val levelsDf = nLevels.toDF()
      .withColumnRenamed(NormalizationColumns.ENTITY_ID, "uniqueId")
      .withColumnRenamed(NormalizationColumns.MAPPED_ID, "levelId")

    val langsDf = nLangs.toDF()
      .withColumnRenamed(NormalizationColumns.ENTITY_ID, "uniqueId")
      .withColumnRenamed(NormalizationColumns.MAPPED_ID, "languageId")

    val finalRes = rawData.select(col("vacancyId").as(NormalizationColumns.ENTITY_ID), col("uniqueId"))
      .join(levelsDf, Seq("uniqueId"), "left")
      .join(langsDf, Seq("uniqueId"), "inner") // Язык обязателен
      .select(
        col(NormalizationColumns.ENTITY_ID), // Это наш externalId вакансии
        col("languageId").cast("long"),
        col("levelId").cast("long") // Option обрабатывается спарком автоматически
      ).as[NormLanguageMatch]

    rawData.unpersist(blocking = false)
    finalRes
  }

  private val languageNormalizeService = new NormalizeService(spark, dbAdapter, languageSettings, DimLanguageDef, MappingLanguageDef)
  private val levelNormalizeService = new NormalizeService(spark, dbAdapter, levelSettings, DimLanguageLevelDef, MappingLanguageLevelDef)
}