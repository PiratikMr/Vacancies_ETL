package org.example.headhunter.implement

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.model.structures.FuzzyMatcherConf
import org.example.core.config.schema.SchemaRegistry.DataBase.Entities
import org.example.core.config.schema.SchemaRegistry.Internal.RawVacancy
import org.example.core.etl.Transformer
import org.example.core.normalization.factory.NormalizerFactory
import org.example.core.objects.NormalizersEnum._
import org.example.core.normalization.api.NormalizationTask.{Exact, ExtractTags}
import org.example.core.normalization.service.NormalizationOrchestrator

class HHTransformer(
                     areas: DataFrame,
                     dbAdapter: DataBaseAdapter,
                     fuzzyConf: FuzzyMatcherConf
                   ) extends Transformer {

  override def toRows(spark: SparkSession, rawDS: Dataset[String]): DataFrame =
    spark.read.schema(HHTransformer.scheme).json(rawDS)

  override def transform(spark: SparkSession, rawDF: DataFrame): DataFrame = {

    val dfWithAreas = rawDF.join(
      areas,
      rawDF("area.id") === areas("area_id"),
      "left"
    )

    println(s"КОЛВО: ${rawDF.count()}")

    dfWithAreas
      .withColumn("parsed_published_at", to_timestamp(col("published_at"), "yyyy-MM-dd'T'HH:mm:ss+0300"))

      .select(
        col("id").as(RawVacancy.externalId.name),
        lit("HeadHunter").as(RawVacancy.platform.name),
        col("employer.name").as(RawVacancy.employer.name),
        col("salary_range.currency").as(RawVacancy.currency.name),
        col("experience.name").as(RawVacancy.experience.name),

        col("address.lat").as(RawVacancy.latitude.name),
        col("address.lng").as(RawVacancy.longitude.name),

        col("salary_range.from").as(RawVacancy.salaryFrom.name),
        col("salary_range.to").as(RawVacancy.salaryTo.name),

        col("parsed_published_at").as(RawVacancy.publishedAt.name),
        col("name").as(RawVacancy.title.name),
        col("description").as(RawVacancy.description.name),
        col("alternate_url").as(RawVacancy.url.name),

        array(col("employment.name")).as(RawVacancy.employments.name),
        array(col("schedule.name")).as(RawVacancy.schedules.name),

        array(
          struct(
            col("country").as(RawVacancy.locationCountry.name),
            col("region").as(RawVacancy.locationRegion.name)
          )
        ).as(RawVacancy.locations.name),

        functions.transform(
          col("languages"),
          lang => struct(
            lang.getField("level").getField("name").as(RawVacancy.languageLevel.name),
            lang.getField("name").as(RawVacancy.languageLanguage.name)
          )
        ).as(RawVacancy.languages.name),

        col("professional_roles.name").as(RawVacancy.fields.name),
        col("key_skills.name").as(RawVacancy.skills.name),
      )
      .repartition(10)
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
        PLATFORM,
        SCHEDULES,
        SKILLS,
        LANGUAGES
        /*ExtractTags(GRADES, RawVacancy.description.name)*/
      ), transformedData)
  }
}

object HHTransformer {
  private val scheme = StructType(Seq(

    StructField("address", StructType(Seq(
      StructField("lat", DoubleType),
      StructField("lng", DoubleType),
      StructField("metro_stations", ArrayType(StructType(Seq(
        StructField("station_name", StringType)
      ))))
    ))),

    StructField("alternate_url", StringType),

    //    StructField("archived", BooleanType),

    StructField("area", StructType(Seq(
      StructField("id", StringType)
    ))),

    //    StructField("driver_license_types", ArrayType(StructType(Seq(
    //      StructField("id", StringType)
    //    )))),

    StructField("employer", StructType(Seq(
      StructField("id", StringType),
      StructField("name", StringType),
      StructField("trusted", BooleanType)
    ))),

    StructField("employment", StructType(Seq(
      StructField("name", StringType)
    ))),

    StructField("experience", StructType(Seq(
      StructField("name", StringType)
    ))),

    StructField("id", StringType),

    StructField("key_skills", ArrayType(StructType(Seq(
      StructField("name", StringType)
    )))),

    StructField("languages", ArrayType(StructType(Seq(
      StructField("level", StructType(Seq(
        StructField("name", StringType)
      ))),
      StructField("name", StringType)
    )))),

    StructField("name", StringType),

    //    StructField("night_shifts", BooleanType),

    StructField("professional_roles", ArrayType(StructType(Seq(
      StructField("name", StringType)
    )))),

    StructField("description", StringType),

    StructField("published_at", StringType),

    StructField("salary_range", StructType(Seq(
      StructField("currency", StringType),
      StructField("frequency", StructType(Seq(
        StructField("name", StringType),
      ))),
      StructField("from", DoubleType),
      StructField("gross", BooleanType),
      StructField("mode", StructType(Seq(
        StructField("name", StringType),
      ))),
      StructField("to", DoubleType)
    ))),

    StructField("schedule", StructType(Seq(
      StructField("name", StringType)
    )))
  ))
}