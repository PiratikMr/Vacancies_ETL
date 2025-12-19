package org.example.headhunter.implement

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.config.FolderName.FolderName
import org.example.core.Interfaces.ETL.Transformer
import org.example.headhunter.config.HHFolderNames

class HHTransformer(
                   transformPartition: Int
                   ) extends Transformer {

  override def toRows(spark: SparkSession, rawDS: Dataset[String]): DataFrame =
    spark.read.schema(HHTransformer.scheme).json(rawDS)

  override def transform(spark: SparkSession, rawDF: DataFrame): Map[FolderName, DataFrame] =
    {

      val transformedDF: DataFrame = rawDF
        .withColumn("address_lat", col("address").getField("lat"))
        .withColumn("address_lng", col("address").getField("lng"))
        .withColumn("address_has_metro", col("address").getField("metro_stations").isNotNull)

        .withColumn("url", col("alternate_url"))

        .withColumn("is_active", not(col("archived")))

        .withColumn("area_id", col("area").getField("id").cast(LongType))

        .withColumn("employer_id", col("employer").getField("id"))
        .withColumn("employer_name", col("employer").getField("name"))
        .withColumn("employer_trusted", col("employer").getField("trusted"))

        .withColumn("employment_id", col("employment").getField("id"))

        .withColumn("experience_id", col("experience").getField("id"))

        .withColumn("id", col("id").cast(LongType))

        .withColumn("title", col("name"))

        .withColumn("are_night_shifts", col("night_shifts"))

        .withColumn("roles", explode(col("professional_roles")))
        .withColumn("role_id", col("roles").getField("id").cast(LongType))

        .withColumn("published_at", to_timestamp(col("published_at"), "yyyy-MM-dd'T'HH:mm:ss+0300"))

        .withColumn("salary_currency_id", col("salary_range").getField("currency"))
        .withColumn("salary_frequency", col("salary_range").getField("frequency").getField("name"))
        .withColumn("salary_from", col("salary_range").getField("from"))
        .withColumn("salary_could_gross", col("salary_range").getField("gross"))
        .withColumn("salary_mode", col("salary_range").getField("mode").getField("name"))
        .withColumn("salary_to", col("salary_range").getField("to"))

        .withColumn("schedule_id", col("schedule").getField("id"))

        .dropDuplicates("id")

      val vacanciesDF: DataFrame = transformedDF
        .select("address_lat", "address_lng", "address_has_metro", "url", "is_active", "area_id", "employer_id",
          "employment_id", "experience_id", "id", "title", "are_night_shifts", "role_id", "published_at",
          "salary_currency_id", "salary_frequency", "salary_from", "salary_could_gross", "salary_mode", "salary_to",
          "schedule_id")
        .repartition(transformPartition)

      val skillsDF: DataFrame = transformedDF
        .select(col("id"), explode(col("key_skills")).as("skills")).select(col("id"), col("skills.name").as("name"))
        .repartition(1)

      val languagesDF: DataFrame = transformedDF
        .select(col("id"), explode(col("languages")).as("lang"))
        .select(col("id"), col("lang.name").as("name"), col("lang.level.name").as("level"))
        .repartition(1)

      val employersDF: DataFrame = transformedDF
        .select(col("employer_id").as("id"), col("employer_name").as("name"), col("employer_trusted").as("trusted"))
        .filter(col("id").isNotNull).dropDuplicates("id")
        .repartition(1)

      Map(
        FolderName.Vacancies -> vacanciesDF,
        FolderName.Skills -> skillsDF,
        HHFolderNames.languages.stage -> languagesDF,
        HHFolderNames.employers.stage -> employersDF
      )
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

    StructField("archived", BooleanType),

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
      StructField("id", StringType)
    ))),

    StructField("experience", StructType(Seq(
      StructField("id", StringType)
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

    StructField("night_shifts", BooleanType),

    StructField("professional_roles", ArrayType(StructType(Seq(
      StructField("id", StringType)
    )))),

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
      StructField("id", StringType)
    )))
  ))
}