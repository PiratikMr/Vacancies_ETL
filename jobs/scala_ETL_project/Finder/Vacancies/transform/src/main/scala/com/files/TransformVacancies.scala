package com.files

import org.apache.spark.sql.functions.{col, concat, explode, lit, to_timestamp, when}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object TransformVacancies extends App with SparkApp {

  private val conf = new ProjectConfig(args) {
    lazy val transformPartitions: Int = getFromConfFile[Int]("transformPartitions")

    verify()
  }
  private val spark: SparkSession = defineSession(conf.sparkConf)

  private val scheme: StructType = StructType(Seq(
    StructField("id", LongType),
    StructField("title", StringType),
    StructField("employment_type", StringType),
    StructField("salary_from", LongType),
    StructField("salary_to", LongType),
    StructField("currency_symbol", StringType),
    StructField("created_at", StringType),
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
  private val rawDF: DataFrame = {
    val ds: Dataset[String] = spark.read.textFile(conf.fsConf.getPath(FolderName.RawVacancies))
    spark.read.schema(scheme).json(ds)
  }

  private val transformedDF: DataFrame = rawDF
    .withColumn("published_at", to_timestamp(col("created_at")))
    .withColumn("salary_currency_id", when(col("currency_symbol") === "RUB", lit("RUR")).otherwise(col("currency_symbol")))
    .withColumn("url", concat(lit("https://finder.work/vacancies/"), col("id")))
    .withColumn("employer", col("company.title"))
    .withColumn("address_lat", col("place.lat").cast(DoubleType))
    .withColumn("address_lng", col("place.lon").cast(DoubleType))
    .withColumn("is_active", lit(true))
    .dropDuplicates("id")

  private val vacanciesDF: DataFrame = transformedDF.select("id", "title", "employment_type", "salary_from", "salary_to",
    "salary_currency_id", "published_at", "url", "experience", "distant_work", "employer", "address_lat", "address_lng",
    "is_active").repartition(conf.transformPartitions)

  private val locationsDF: DataFrame = transformedDF.select(col("id"), explode(col("locations")).as("location"))
    .select(col("id"), col("location.name").as("name"), col("location.country.name").as("country"))
    .repartition(1)

  private val fieldsDF: DataFrame = transformedDF.select(col("id"), explode(col("professions")).as("role"))
    .select(col("id"), col("role.title").as("name"))
    .repartition(1)


  HDFSHandler.saveParquet(vacanciesDF, conf.fsConf.getPath(FolderName.Vacancies))
  HDFSHandler.saveParquet(locationsDF, conf.fsConf.getPath(FolderName.Locations))
  HDFSHandler.saveParquet(fieldsDF, conf.fsConf.getPath(FolderName.Fields))


  spark.stop()

}
