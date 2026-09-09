package org.example.core.normalization.service

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.model.structures.FuzzyMatcherConf
import org.example.core.etl.model.{NormalizationResult, NormalizedLanguage, NormalizedVacancy, Vacancy, VacancyColumns}
import org.example.core.normalization.api.NormalizationTask
import org.example.core.normalization.engine.similarity.impl.TextNormalizer
import org.example.core.normalization.factory.NormalizerFactory
import org.example.core.normalization.model.NormalizersEnum._
import org.example.core.normalization.model.{MatchLogPart, NormalizationColumns, NormalizationOutput}

class NormalizationOrchestrator(spark: SparkSession,
                                dbAdapter: DataBaseAdapter,
                                conf: FuzzyMatcherConf) {

  import spark.implicits._

  def normalize(tasks: Seq[NormalizationTask], ds: Dataset[Vacancy]): NormalizationResult = {

    val initialDf = ds.toDF()
      .repartition(col(VacancyColumns.EXTERNAL_ID))
      .withColumn(VacancyColumns.DESCRIPTION_HASH,
        md5(TextNormalizer.simpleNormalize(col(VacancyColumns.DESCRIPTION)))
      )

    if (tasks.isEmpty) {
      return NormalizationResult(buildFinalContract(initialDf), Seq.empty)
    }

    val outputs: Seq[NormalizationOutput] = tasks.map { task =>
      NormalizerFactory.createCommand(task, spark, dbAdapter, conf).execute(ds)
    }

    val mappingDfs: Seq[DataFrame] = outputs.map(
      _.mappings.withColumnRenamed(NormalizationColumns.ENTITY_ID, VacancyColumns.EXTERNAL_ID)
    )

    val matchLogs: Seq[MatchLogPart] = outputs.flatMap(_.matchLogs).map { part =>
      part.copy(rows = part.rows.withColumnRenamed(NormalizationColumns.ENTITY_ID, VacancyColumns.EXTERNAL_ID))
    }

    val combinedMappings = mappingDfs.reduce { (df1, df2) =>
      df1.join(df2, Seq(VacancyColumns.EXTERNAL_ID), "full_outer")
    }

    val enrichedDf = initialDf.join(combinedMappings, Seq(VacancyColumns.EXTERNAL_ID), "left")

    NormalizationResult(buildFinalContract(enrichedDf), matchLogs)
  }

  private def buildFinalContract(enrichedDf: DataFrame): Dataset[NormalizedVacancy] = {
    def safeCol(colName: String) = {
      if (enrichedDf.columns.contains(colName)) col(colName) else lit(null)
    }

    val languagesCol = if (enrichedDf.columns.contains(LANGUAGES.mappedIdCol)) {
      transform(
        col(LANGUAGES.mappedIdCol),
        lang => struct(
          lang.getField(VacancyColumns.LANGUAGE_ID).as("languageId"),
          lang.getField(VacancyColumns.LEVEL_ID).as("levelId")
        )
      )
    } else {
      lit(null)
    }

    enrichedDf.select(
      col(VacancyColumns.EXTERNAL_ID),
      col(VacancyColumns.TITLE),
      col(VacancyColumns.DESCRIPTION),
      col(VacancyColumns.DESCRIPTION_HASH),
      col(VacancyColumns.URL),
      col(VacancyColumns.LATITUDE),
      col(VacancyColumns.LONGITUDE),
      col(VacancyColumns.SALARY_FROM),
      col(VacancyColumns.SALARY_TO),

      safeCol(PLATFORM.mappedIdCol).cast("long").as(VacancyColumns.PLATFORM_ID),
      safeCol(EMPLOYER.mappedIdCol).cast("long").as(VacancyColumns.EMPLOYER_ID),
      safeCol(CURRENCY.mappedIdCol).cast("long").as(VacancyColumns.CURRENCY_ID),
      safeCol(EXPERIENCE.mappedIdCol).cast("long").as(VacancyColumns.EXPERIENCE_ID),

      coalesce(safeCol(EMPLOYMENTS.mappedIdCol), typedLit(Seq.empty[Long])).as(VacancyColumns.EMPLOYMENT_IDS),
      coalesce(safeCol(SCHEDULES.mappedIdCol), typedLit(Seq.empty[Long])).as(VacancyColumns.SCHEDULE_IDS),
      coalesce(safeCol(FIELDS.mappedIdCol), typedLit(Seq.empty[Long])).as(VacancyColumns.FIELD_IDS),
      coalesce(safeCol(GRADES.mappedIdCol), typedLit(Seq.empty[Long])).as(VacancyColumns.GRADE_IDS),
      coalesce(safeCol(SKILLS.mappedIdCol), typedLit(Seq.empty[Long])).as(VacancyColumns.SKILL_IDS),

      coalesce(languagesCol, typedLit(Seq.empty[NormalizedLanguage])).as(VacancyColumns.LANGUAGES),

      coalesce(safeCol(LOCATIONS.mappedIdCol), typedLit(Seq.empty[Long])).as(VacancyColumns.LOCATIONS),

      col(VacancyColumns.PUBLISHED_AT)
    ).as[NormalizedVacancy]
  }
}