package org.example.core.normalization.service

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.model.structures.FuzzyMatcherConf
import org.example.core.config.schema.SchemaRegistry.Internal.{NormalizedVacancy => SchemaNormalizedVacancy}
import org.example.core.etl.model.{NormalizedLanguage, NormalizedVacancy, Vacancy, VacancyColumns}
import org.example.core.normalization.api.NormalizationTask
import org.example.core.normalization.factory.NormalizerFactory
import org.example.core.normalization.model.{NormCandidate, NormalizationColumns, NormalizersEnum}
import org.example.core.normalization.model.NormalizersEnum._

class NormalizationOrchestrator(spark: SparkSession,
                                dbAdapter: DataBaseAdapter,
                                conf: FuzzyMatcherConf) {

  import spark.implicits._

  def normalize(tasks: Seq[NormalizationTask], ds: Dataset[Vacancy]): Dataset[NormalizedVacancy] = {

    val initialDf = ds.toDF()

    val enrichedDf = tasks.foldLeft(initialDf) { (resDf, task) =>
      val nType = task.nType

      val mappedDf = nType match {

        // --- 1. ЯЗЫКИ (сложный объект) ---
        case LANGUAGES =>
          val languageNormalizer = NormalizerFactory.getLanguageNormalizer(spark, dbAdapter, conf.get(LANGUAGES_LEVEL), conf.get(LANGUAGES))
          val normalized = languageNormalizer.normalize(ds)
          // Группируем языки в массив структур для джоина
          normalized.toDF()
            .groupBy(NormalizationColumns.ENTITY_ID)
            .agg(collect_list(
              struct(
                col("languageId").as(SchemaNormalizedVacancy.languageLanguage.name),
                col("levelId").as(SchemaNormalizedVacancy.languageLevel.name)
              )
            ).as("mapped_languages"))

        // --- 2. ЛОКАЦИИ (иерархия) ---
        case LOCATIONS =>
          val locNormalizer = NormalizerFactory.getLocationsNormalizer(spark, dbAdapter, conf.get(LOCATIONS), conf.get(COUNTRY))
          val normalized = locNormalizer.normalize(ds)
          // Так как LOCATIONS - это массив, агрегируем flat-результат в массив
          normalized.toDF()
            .groupBy(NormalizationColumns.ENTITY_ID)
            .agg(collect_list(NormalizationColumns.MAPPED_ID).as(nType.mappedIdCol))

        // --- 3. ПРОСТЫЕ СПРАВОЧНИКИ ---
        case simple: GroupNonHierarchical =>
          val service = NormalizerFactory.getNormalizeService(spark, dbAdapter, conf.get(simple), simple)

          // Шаг А: Извлекаем сырых кандидатов прямо в оркестраторе!
          val rawCandidates = if (simple.isArray) {
            initialDf.select(
              col(VacancyColumns.EXTERNAL_ID).as(NormalizationColumns.ENTITY_ID),
              explode_outer(col(simple.sourceCol)).as(NormalizationColumns.RAW_VALUE),
              lit(null).cast("string").as(NormalizationColumns.PARENT_ID) // Добавлено
            )
          } else {
            initialDf.select(
              col(VacancyColumns.EXTERNAL_ID).as(NormalizationColumns.ENTITY_ID),
              col(simple.sourceCol).cast("string").as(NormalizationColumns.RAW_VALUE),
              lit(null).cast("string").as(NormalizationColumns.PARENT_ID) // Добавлено
            )
          }

          val cleanCandidates = rawCandidates
            .filter(col(NormalizationColumns.RAW_VALUE).isNotNull)
            .as[NormCandidate]

          // Шаг Б: Нормализуем через сервис
          val normalizedDs = task match {
            case NormalizationTask.Standard(_) => service.mapSimple(cleanCandidates, withCreate = true)
            case NormalizationTask.Exact(_)    => service.mapSimple(cleanCandidates, withCreate = false)
            case NormalizationTask.ExtractTags(_, _) => service.extractTags(cleanCandidates) // sourceCol из таски пока игнорим, так как всё есть в enum
          }

          // Шаг В: Агрегируем результаты
          if (simple.isArray) {
            normalizedDs.toDF()
              .groupBy(NormalizationColumns.ENTITY_ID)
              .agg(collect_list(NormalizationColumns.MAPPED_ID).as(simple.mappedIdCol))
          } else {
            normalizedDs.toDF()
              .select(
                col(NormalizationColumns.ENTITY_ID),
                col(NormalizationColumns.MAPPED_ID).as(simple.mappedIdCol)
              )
          }
      }

      // Джоиним результат текущей таски к общей таблице
      resDf.join(mappedDf.withColumnRenamed(NormalizationColumns.ENTITY_ID, VacancyColumns.EXTERNAL_ID), Seq(VacancyColumns.EXTERNAL_ID), "left")
    }

    // Финальная сборка контракта (остается без изменений)
    enrichedDf.select(
      col(VacancyColumns.EXTERNAL_ID),
      col(VacancyColumns.TITLE),
      col(VacancyColumns.URL),
      col(VacancyColumns.LATITUDE),
      col(VacancyColumns.LONGITUDE),
      col(VacancyColumns.SALARY_FROM),
      col(VacancyColumns.SALARY_TO),

      col(NormalizersEnum.PLATFORM.mappedIdCol).as(VacancyColumns.PLATFORM_ID),
      col(NormalizersEnum.EMPLOYER.mappedIdCol).as(VacancyColumns.EMPLOYER_ID),
      col(NormalizersEnum.CURRENCY.mappedIdCol).as(VacancyColumns.CURRENCY_ID),
      col(NormalizersEnum.EXPERIENCE.mappedIdCol).as(VacancyColumns.EXPERIENCE_ID),

      coalesce(col(NormalizersEnum.EMPLOYMENTS.mappedIdCol), typedLit(Seq.empty[Long])).as(VacancyColumns.EMPLOYMENT_IDS),
      coalesce(col(NormalizersEnum.SCHEDULES.mappedIdCol), typedLit(Seq.empty[Long])).as(VacancyColumns.SCHEDULE_IDS),
      coalesce(col(NormalizersEnum.FIELDS.mappedIdCol), typedLit(Seq.empty[Long])).as(VacancyColumns.FIELD_IDS),
      coalesce(col(NormalizersEnum.GRADES.mappedIdCol), typedLit(Seq.empty[Long])).as(VacancyColumns.GRADE_IDS),
      coalesce(col(NormalizersEnum.SKILLS.mappedIdCol), typedLit(Seq.empty[Long])).as(VacancyColumns.SKILL_IDS),

      coalesce(
        transform(
          col("mapped_languages"),
          lang => struct(
            lang.getField(SchemaNormalizedVacancy.languageLanguage.name).as("languageId"),
            lang.getField(SchemaNormalizedVacancy.languageLevel.name).as("levelId")
          )
        ),
        typedLit(Seq.empty[NormalizedLanguage])
      ).as(VacancyColumns.LANGUAGES),

      coalesce(col(LOCATIONS.mappedIdCol), typedLit(Seq.empty[Long])).as(VacancyColumns.LOCATIONS),

      col(VacancyColumns.PUBLISHED_AT)
    ).as[NormalizedVacancy]
  }
}