package org.example.finder.implement

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.model.structures.FuzzyMatcherConf
import org.example.core.config.schema.SchemaRegistry.Internal.RawVacancy
import org.example.core.etl.Transformer
import org.example.core.normalization.api.NormalizationTask.ExtractTags
import org.example.core.normalization.service.NormalizationOrchestrator
import org.example.core.normalization.model.NormalizersEnum._

class FinderTransformer(dbAdapter: DataBaseAdapter,
                        fuzzyConf: FuzzyMatcherConf) extends Transformer {

  override def toRows(spark: SparkSession, rawDS: Dataset[String]): DataFrame =
    spark.read.schema(FinderTransformer.schema).json(rawDS)

  override def transform(spark: SparkSession, rawDF: DataFrame): DataFrame = {
    rawDF
      .select(
        col("id").cast(StringType).as(RawVacancy.externalId.name),
        lit("Finder").as(RawVacancy.platform.name),
        col("company.title").as(RawVacancy.employer.name),
        col("currency_symbol").as(RawVacancy.currency.name),
        col("experience").as(RawVacancy.experience.name),

        col("place.lat").cast(DoubleType).as(RawVacancy.latitude.name),
        col("place.lon").cast(DoubleType).as(RawVacancy.longitude.name),

        col("salary_from").cast(DoubleType).as(RawVacancy.salaryFrom.name),
        col("salary_to").cast(DoubleType).as(RawVacancy.salaryTo.name),

        to_timestamp(col("publication_at")).as(RawVacancy.publishedAt.name),
        col("title").as(RawVacancy.title.name),
        col("description").as(RawVacancy.description.name),
        concat(lit("https://finder.work/vacancies/"), col("id")).as(RawVacancy.url.name),

        array(col("employment_type")).as(RawVacancy.employments.name),

        functions.transform(
          col("locations"),
          loc => struct(
            loc.getField("name").as(RawVacancy.locationRegion.name),
            loc.getField("country").getField("name").as(RawVacancy.locationCountry.name)
          )
        ).as(RawVacancy.locations.name),

        col("professions.title").as(RawVacancy.fields.name)
      )
  }

  override def normalize(spark: SparkSession, transformedData: DataFrame): DataFrame = {
    new NormalizationOrchestrator(spark, dbAdapter, fuzzyConf)
      .normalize(Seq(
        CURRENCY,
        EMPLOYER,
        EMPLOYMENTS,
        EXPERIENCE,
        FIELDS,
        LOCATIONS,
        PLATFORM
//        ExtractTags(SCHEDULES, RawVacancy.description.name),
//        ExtractTags(SKILLS, RawVacancy.description.name),
//        ExtractTags(GRADES, RawVacancy.description.name)
      ), transformedData)
  }
}

object FinderTransformer {

  val schema: StructType = StructType(Seq(
    StructField("id", LongType),
    StructField("title", StringType),
    StructField("employment_type", StringType),
    StructField("salary_from", LongType),
    StructField("salary_to", LongType),
    StructField("currency_symbol", StringType),
    StructField("publication_at", StringType),
    StructField("experience", StringType),
    StructField("distant_work", BooleanType),
    StructField("company", StructType(Seq(
      StructField("title", StringType)
    ))),
    StructField("locations", ArrayType(StructType(Seq(
      StructField("name", StringType),
      StructField("country", StructType(Seq(
        StructField("name", StringType)
      ))),
    )))),
    StructField("place", StructType(Seq(
      StructField("lat", StringType),
      StructField("lon", StringType),
    ))),
    StructField("professions", ArrayType(StructType(Seq(
      StructField("title", StringType)
    )))),
    StructField("description", StringType)
  ))

}
