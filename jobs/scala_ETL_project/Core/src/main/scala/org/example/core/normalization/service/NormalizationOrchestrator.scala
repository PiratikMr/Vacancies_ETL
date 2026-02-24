package org.example.core.normalization.service

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.model.structures.FuzzyMatcherConf
import org.example.core.config.schema.SchemaRegistry.Internal.{NormalizedVacancy => SchemaNormalizedVacancy}
import org.example.core.etl.model.{NormalizedLanguage, NormalizedVacancy, Vacancy, VacancyColumns}
import org.example.core.normalization.api.{NormalizationTask, Normalizer}
import org.example.core.normalization.factory.NormalizerFactory
import org.example.core.normalization.model.NormalizersEnum
import org.example.core.normalization.model.NormalizersEnum._

class NormalizationOrchestrator(spark: SparkSession,
                                dbAdapter: DataBaseAdapter,
                                conf: FuzzyMatcherConf) {

  import spark.implicits._

  def normalize(tasks: Seq[NormalizationTask], ds: Dataset[Vacancy]): Dataset[NormalizedVacancy] = {

    val initialDf = ds.toDF()

    val enrichedDf = tasks.foldLeft(initialDf)((resDf, task) => {
      val normalizer = getNormalizer(task.nType)

      val mappedResult = task match {
        case NormalizationTask.Standard(_) => normalizer.normalize(initialDf)
        case NormalizationTask.Exact(_) => normalizer.matchExactData(initialDf)
        case NormalizationTask.ExtractTags(_, sourceCol) => normalizer.extractTags(initialDf, sourceCol)
      }

      val mappedDf = task.nType match {
        case NormalizersEnum.LANGUAGES =>
          mappedResult.mappedDf
            .withColumnRenamed(SchemaNormalizedVacancy.languages.name, "mapped_languages")
        case _ =>
          mappedResult.mappedDf
            .withColumnRenamed(mappedResult.mappedIdCol, task.nType.mappedIdCol)
      }

      resDf.join(mappedDf, Seq(VacancyColumns.EXTERNAL_ID), "left")
    })

    enrichedDf.select(
      col(VacancyColumns.EXTERNAL_ID),
      col(VacancyColumns.TITLE),
      col(VacancyColumns.URL),

      col(VacancyColumns.LATITUDE),
      col(VacancyColumns.LONGITUDE),

      col(VacancyColumns.SALARY_FROM),
      col(VacancyColumns.SALARY_TO),

      // ИСПРАВЛЕНИЕ: Берем колонки по mappedIdCol (platform_id и т.д.) и переименовываем их под контракт
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

  private def getNormalizer(nType: NormalizerType): Normalizer = {
    nType match {
      case LANGUAGES => NormalizerFactory.getLanguageNormalizer(spark, dbAdapter, conf.get(LANGUAGES_LEVEL), conf.get(LANGUAGES))
      case v: GroupNonHierarchical => NormalizerFactory.getNonHierarchicalNormalizer(spark, dbAdapter, conf.get(nType), v)
      case LOCATIONS => NormalizerFactory.getLocationsNormalizer(spark, dbAdapter, conf.get(LOCATIONS), conf.get(COUNTRY))
    }
  }

}