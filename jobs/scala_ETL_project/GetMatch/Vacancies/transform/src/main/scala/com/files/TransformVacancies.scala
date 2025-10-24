package com.files

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, BooleanType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object TransformVacancies extends App with SparkApp {

  private val conf = new ProjectConfig(args) {
    lazy val transformPartitions: Int = getFromConfFile[Int]("transformPartitions")

    verify()
  }

  private val spark: SparkSession = defineSession(conf.sparkConf)


  private val currencyDF: DataFrame = DBHandler.load(spark, conf.dbConf, FolderName.Currency)
    .select(col("id").as("c_id"), col("code").as("c_code"))

  private val scheme: StructType = StructType(Seq(
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
  private val rawDF: DataFrame = {
    val ds: Dataset[String] = spark.read.textFile(conf.fsConf.getPath(FolderName.RawVacancies))
    spark.read.schema(scheme).json(ds)
  }

  private val transformedDF: DataFrame = rawDF
    .join(currencyDF, rawDF("salary_currency") === currencyDF("c_code"), "left_outer")

    .withColumn("published_at", to_timestamp(col("published_at"), "yyyy-MM-dd"))
    .withColumn("title", col("position"))
    .withColumn("salary_from", col("salary_display_from"))
    .withColumn("salary_to", col("salary_display_to"))
    .withColumn("salary_currency_id", col("c_id"))
    .withColumn("url", concat(lit("https://getmatch.ru"), col("url")))
    .withColumn("english_level", col("english_level.name"))
    .withColumn("employer", col("company.name"))
    .withColumn("level", col("position_level"))
    .withColumn("experience_years", col("required_years_of_experience"))

    .dropDuplicates("id")


  private val vacanciesDF: DataFrame = transformedDF.select("published_at", "is_active", "title", "id", "salary_from",
  "salary_to", "salary_currency_id", "url", "english_level", "remote_options", "office_options", "employer",
      "level", "experience_years")
    .repartition(conf.transformPartitions)

  private val skillsDF: DataFrame = transformedDF.select(col("id"), explode(col("stack")).as("name"))
    .repartition(1)


  HDFSHandler.saveParquet(vacanciesDF, conf.fsConf.getPath(FolderName.Vacancies))
  HDFSHandler.saveParquet(skillsDF, conf.fsConf.getPath(FolderName.Skills))

  spark.stop()

}
