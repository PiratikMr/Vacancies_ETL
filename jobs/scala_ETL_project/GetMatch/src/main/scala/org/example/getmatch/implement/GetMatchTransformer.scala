package org.example.getmatch.implement

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import org.example.config.FolderName.FolderName
import org.example.core.Interfaces.ETL.Transformer

class GetMatchTransformer(
                         transformPartition: Int
                         ) extends Transformer {

  private val schema: StructType = StructType(Seq(
    StructField("published_at", StringType),
    StructField("is_active", BooleanType),
    StructField("position", StringType),
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
    StructField("position_level", StringType),
    StructField("required_years_of_experience", IntegerType)

  ))

  override def toRows(spark: SparkSession, rawDS: Dataset[String]): DataFrame =
    spark.read.schema(schema).json(rawDS)

  override def transform(spark: SparkSession, rawDF: DataFrame): Map[FolderName, DataFrame] =
    {

      val currencyMapColumn = functions.map(
        GetMatchTransformer.currencyMap.flatMap { case (k, v) => Seq(lit(k), lit(v)) }.toSeq: _*
      )

      val transformedDF: DataFrame = rawDF
        .withColumn("published_at", to_timestamp(col("published_at"), "yyyy-MM-dd"))
        .withColumn("title", col("position"))
        .withColumn("salary_from", col("salary_display_from"))
        .withColumn("salary_to", col("salary_display_to"))
        .withColumn("salary_currency_id", currencyMapColumn.getItem(col("salary_currency")))
        .withColumn("url", concat(lit("https://getmatch.ru"), col("url")))
        .withColumn("english_level", col("english_level.name"))
        .withColumn("employer", col("company.name"))
        .withColumn("level", col("position_level"))
        .withColumn("experience_years", col("required_years_of_experience"))

        .dropDuplicates("id")

      val vacanciesDF: DataFrame = transformedDF.select("published_at", "is_active", "title", "id", "salary_from",
          "salary_to", "salary_currency_id", "url", "english_level", "remote_options", "office_options", "employer",
          "level", "experience_years")
        .repartition(transformPartition)

      val skillsDF: DataFrame = transformedDF.select(col("id"), explode(col("stack")).as("name"))
        .repartition(1)

      Map(
        FolderName.Vacancies -> vacanciesDF,
        FolderName.Skills -> skillsDF
      )
    }
}

object GetMatchTransformer {

  private lazy val currencyMap = Map(
    "€" -> "EUR",
    "₽" -> "RUB",
    "$"-> "USD"
  )
}
