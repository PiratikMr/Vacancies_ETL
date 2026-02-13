package org.example.getmatch.implement

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.model.structures.FuzzyMatcherConf
import org.example.core.config.schema.SchemaRegistry.Internal.RawVacancy
import org.example.core.etl.Transformer
import org.example.core.normalization.api.NormalizationTask.ExtractTags
import org.example.core.normalization.service.NormalizationOrchestrator
import org.example.core.objects.NormalizersEnum._
import org.example.getmatch.implement.GetMatchTransformer._

class GetMatchTransformer(dbAdapter: DataBaseAdapter,
                          fuzzyConf: FuzzyMatcherConf,
                          transformPartition: Int) extends Transformer {


  override def toRows(spark: SparkSession, rawDS: Dataset[String]): DataFrame =
    spark.read.schema(schema).json(rawDS)

  override def transform(spark: SparkSession, rawDF: DataFrame): DataFrame = {

    val currencyMapColumn = functions.map(
      GetMatchTransformer.currencyMap.flatMap { case (k, v) => Seq(lit(k), lit(v)) }.toSeq: _*
    )

    rawDF.select(
      col("id").cast(StringType).as(RawVacancy.externalId.name),
      lit("GetMatch").as(RawVacancy.platform.name),
      col("company.name").as(RawVacancy.employer.name),
      currencyMapColumn.getItem(col("salary_currency")).as(RawVacancy.currency.name),
      col("required_years_of_experience").cast(StringType).as(RawVacancy.experience.name),

      col("salary_display_from").cast(DoubleType).as(RawVacancy.salaryFrom.name),
      col("salary_display_to").cast(DoubleType).as(RawVacancy.salaryTo.name),

      to_timestamp(col("published_at"), "yyyy-MM-dd").as(RawVacancy.publishedAt.name),
      col("position").as(RawVacancy.title.name),
      col("offer_description").as(RawVacancy.description.name),
      concat(lit("https://getmatch.ru"), col("url")).as(RawVacancy.url.name),

      array(col("seniority")).as(RawVacancy.grades.name),

      array_remove(
        array(
          when(col("remote_options").isNotNull, lit("Удаленная работа"))
            .otherwise(lit(null)),
          when(col("office_options").isNotNull, lit("Полный день"))
            .otherwise(lit(null))
        ),
        null
      ).as(RawVacancy.schedules.name),

      filter(functions.transform(
        array(col("english_level.name")),
        lang => struct(
          lit("Английский").as(RawVacancy.languageLanguage.name),
          lang.as(RawVacancy.languageLevel.name)
        )
      ),
        x => x.getField(RawVacancy.languageLevel.name).isNotNull
      ).as(RawVacancy.languages.name),

      functions.transform(
        col("display_locations"),
        loc => struct(
          loc.getField("city").as(RawVacancy.locationRegion.name),
          loc.getField("country").as(RawVacancy.locationCountry.name)
        )
      ),

      flatten(
        functions.transform(
          col("stack"),
          stack_item => split(trim(stack_item), "\\s+/\\s+")
        )
      ).as(RawVacancy.skills.name)
    )
  }

  override def normalize(spark: SparkSession, transformedData: DataFrame): DataFrame = {

    new NormalizationOrchestrator(spark, dbAdapter, fuzzyConf)
      .normalize(Seq(
        PLATFORM,
        EMPLOYER,
        CURRENCY,
        EXPERIENCE,
        GRADES,
        SCHEDULES,
        LANGUAGES,
        LOCATIONS,
        SKILLS,
        ExtractTags(FIELDS, RawVacancy.description.name),
        ExtractTags(EMPLOYMENTS, RawVacancy.description.name)
      ), transformedData)

  }
}

object GetMatchTransformer {

  private lazy val currencyMap = Map(
    "€" -> "EUR",
    "₽" -> "RUB",
    "$"-> "USD"
  )

  private val schema: StructType = StructType(Seq(
    StructField("published_at", StringType),
    StructField("position", StringType),
    StructField("offer_description", StringType),
    StructField("id", LongType),
    StructField("salary_display_from", LongType),
    StructField("salary_display_to", LongType),
    StructField("salary_currency", StringType),
    StructField("stack", ArrayType(StringType)),
    StructField("url", StringType),
    StructField("english_level", StructType(Seq(
      StructField("name", StringType)
    ))),
    StructField("remote_options", StringType),
    StructField("office_options", StringType),
    StructField("company", StructType(Seq(
      StructField("name", StringType)
    ))),
    StructField("seniority", StringType),
    StructField("required_years_of_experience", IntegerType),
    StructField("display_locations", ArrayType(StructType(Seq(
      StructField("city", StringType),
      StructField("country", StringType)
    ))))

  ))
}