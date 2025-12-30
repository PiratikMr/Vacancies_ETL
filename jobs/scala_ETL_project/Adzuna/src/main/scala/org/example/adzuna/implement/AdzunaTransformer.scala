package org.example.adzuna.implement

import org.apache.spark.sql.functions.{col, concat, element_at, explode, lit, size, to_timestamp, when}
import org.apache.spark.sql.types.{ArrayType, DoubleType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.adzuna.implement.AdzunaTransformer.schema
import org.example.config.FolderName.{CustomDomain, FolderName, Stage}
import org.example.core.Interfaces.ETL.Transformer

class AdzunaTransformer(
                       currency: String,
                       urlDomain: String,
                       locationTag: String,
                       transformPartition: Int
                       ) extends Transformer {

  override def toRows(spark: SparkSession, rawDS: Dataset[String]): DataFrame =
    spark.read.schema(StructType(Seq(StructField("results", ArrayType(schema))))).json(rawDS)

  override def transform(spark: SparkSession, rawDF: DataFrame): Map[FolderName, DataFrame] = {

    val _currency = currency
    val _urlDomain = urlDomain

    val transformedDF = rawDF.select(explode(col("results")).as("rslt")).select("rslt.*")
      .withColumn("id", col("id").cast(LongType))
      .withColumn("published_at", to_timestamp(col("created")))
      .withColumn("closed_at", lit(null).cast(TimestampType))

      .withColumn("address_lat", col("latitude"))
      .withColumn("address_lng", col("longitude"))

      .withColumn("country", col("location.area").getItem(0))
      .withColumn("region",
        when(size(col("location.area")) > 1, element_at(col("location.area"), -1))
          .otherwise(lit(null).cast(StringType))
      )

      .withColumn("employer", col("company.display_name"))

      .withColumn("url", concat(lit("https://www.adzuna."), lit(_urlDomain), lit("/details/"), col("id")))

      .withColumn("salary_from", col("salary_min"))
      .withColumn("salary_to",col("salary_max"))
      .withColumn("salary_currency_id", lit(_currency))

      .withColumn("employment_type", col("contract_time"))

      .repartition(transformPartition)

    val vacanciesDF = transformedDF.dropDuplicates("id").select("id", "title", "published_at", "closed_at", "address_lat", "address_lng",
    "country", "region", "employer", "url", "salary_from", "salary_to", "salary_currency_id", "employment_type")

    Map(
      FolderName(Stage, CustomDomain(locationTag), "Vacancies") -> vacanciesDF
    )
  }
}

object AdzunaTransformer {

  private val schema = StructType(Seq(
    StructField("id", StringType),
    StructField("title", StringType),
    StructField("created", StringType),
    StructField("latitude", DoubleType),
    StructField("longitude", DoubleType),
    StructField("location", StructType(Seq(
      StructField("area", ArrayType(StringType))
    ))),
    StructField("company", StructType(Seq(
      StructField("display_name", StringType)
    ))),
    StructField("salary_min", DoubleType),
    StructField("salary_max", DoubleType),
    StructField("contract_time", StringType)
  ))

}
