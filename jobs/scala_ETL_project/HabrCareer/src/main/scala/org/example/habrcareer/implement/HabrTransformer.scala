package org.example.habrcareer.implement

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.config.FolderName.FolderName
import org.example.core.Interfaces.ETL.Transformer

class HabrTransformer(
                     transformPartition: Int
                     ) extends Transformer {

  override def toRows(spark: SparkSession, rawDS: Dataset[String]): DataFrame =
    spark.read.schema(StructType(Seq(StructField("list", ArrayType(HabrTransformer.scheme))))).json(rawDS)

  override def transform(spark: SparkSession, rawDF: DataFrame): Map[FolderName, DataFrame] =
    {

      val transformedDF = rawDF.select(explode(col("list")).as("lst")).select("lst.*")
        .withColumn("remote_work", col("remoteWork"))
        .withColumn("grade", col("salaryQualification.title"))
        .withColumn("published_at", to_timestamp(col("publishedDate.date"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
        .withColumn("employer", col("company.title"))
        .withColumn("employment_type", col("employment"))
        .withColumn("salary_from", col("salary.from"))
        .withColumn("salary_to", col("salary.to"))
        .withColumn("salary_currency_id", upper(col("salary.currency")))
        .withColumn("url", concat(lit("https://career.habr.com/vacancies/"), col("id")))
        .withColumn("closed_at", lit(null).cast(TimestampType))

      val vacanciesDF: DataFrame = transformedDF.select("id", "title", "remote_work", "grade", "published_at",
          "employer", "employment_type", "salary_from", "salary_to", "salary_currency_id", "url", "closed_at")
        .repartition(transformPartition)

      val fieldsDF: DataFrame = transformedDF
        .select(col("id"), explode(col("divisions")).as("fields"))
        .select(col("id"), col("fields.title").as("name"))
        .repartition(1)

      val skillsDF: DataFrame = transformedDF
        .select(col("id"), explode(col("skills")).as("skill"))
        .select(col("id"), col("skill.title").as("name"))
        .repartition(1)

      val locationsDF: DataFrame = transformedDF
        .select(col("id"), explode(col("locations")).as("mlocations"))
        .select(col("id"), col("mlocations.title").as("name"))
        .repartition(1)

      Map(
        FolderName.Vacancies -> vacanciesDF,
        FolderName.Fields -> fieldsDF,
        FolderName.Skills -> skillsDF,
        FolderName.Locations -> locationsDF
      )
    }
}

object HabrTransformer {
  private val scheme: StructType = StructType(Seq(
    StructField("id", LongType),
    StructField("title", StringType),
    StructField("remoteWork", BooleanType),
    StructField("salaryQualification", StructType(Seq(
      StructField("title", StringType)
    ))),
    StructField("publishedDate", StructType(Seq(
      StructField("date", StringType)
    ))),
    // location
    StructField("company", StructType(Seq(
      StructField("title", StringType)
    ))),
    StructField("employment", StringType),
    StructField("salary", StructType(Seq(
      StructField("from", LongType),
      StructField("to", LongType),
      StructField("currency", StringType)
    ))),
    StructField("divisions", ArrayType(StructType(Seq(
      StructField("title", StringType)
    )))),
    StructField("skills", ArrayType(StructType(Seq(
      StructField("title", StringType)
    )))),
    StructField("locations", ArrayType(StructType(Seq(
      StructField("title", StringType)
    ))))
  ))
}