package org.example.finder.ETL

import org.apache.spark.sql.functions.{col, concat, explode, lit, to_timestamp, when}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.config.FolderName.FolderName
import org.example.core.Interfaces.ETL.Transformer

class FinderTransformer(
                       transformPartition: Int
                       ) extends Transformer {

  override def toRows(spark: SparkSession, rawDS: Dataset[String]): DataFrame =
    spark.read.schema(FinderTransformer.schema).json(rawDS)

  override def transform(spark: SparkSession, rawDF: DataFrame): Map[FolderName, DataFrame] =
    {
      val transformedDF: DataFrame = rawDF
        .withColumn("published_at", to_timestamp(col("publication_at")))
        .withColumn("salary_currency_id",
          when(col("currency_symbol") === "RUB", lit("RUR"))
            .when(col("currency_symbol") === "BYN", lit("BYR"))
            .otherwise(col("currency_symbol"))
        )
        .withColumn("url", concat(lit("https://finder.work/vacancies/"), col("id")))
        .withColumn("employer", col("company.title"))
        .withColumn("address_lat", col("place.lat").cast(DoubleType))
        .withColumn("address_lng", col("place.lon").cast(DoubleType))
        .withColumn("is_active", lit(true))
        .dropDuplicates("id")

      val vacanciesDF: DataFrame = transformedDF.select("id", "title", "employment_type", "salary_from", "salary_to",
        "salary_currency_id", "published_at", "url", "experience", "distant_work", "employer", "address_lat", "address_lng",
        "is_active").repartition(transformPartition)

      val locationsDF: DataFrame = transformedDF.select(col("id"), explode(col("locations")).as("location"))
        .select(col("id"), col("location.name").as("name"), col("location.country.name").as("country"))
        .repartition(1)

      val fieldsDF: DataFrame = transformedDF.select(col("id"), explode(col("professions")).as("role"))
        .select(col("id"), col("role.title").as("name"))
        .repartition(1)


      Map(
        FolderName.Vacancies -> vacanciesDF,
        FolderName.Locations -> locationsDF,
        FolderName.Fields -> fieldsDF
      )
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
    ))))
  ))

}
