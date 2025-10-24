package com.files

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

object TransformVacancies extends App with SparkApp {

  private val conf = new ProjectConfig(args) {
    lazy val transformPartitions: Int = getFromConfFile[Int]("transformPartitions")

    verify()
  }


  private val spark: SparkSession = defineSession(conf.sparkConf)

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
    )))),
    StructField("archived", BooleanType)
  ))
  private val rawDF: DataFrame = {
    val ds: Dataset[String] = spark.read.textFile(conf.fsConf.getPath(FolderName.RawVacancies))
    spark.read.schema(StructType(Seq(StructField("list", ArrayType(scheme))))).json(ds)
  }

  private val transformedDF: DataFrame = rawDF.select(explode(col("list")).as("lst")).select("lst.*")
    .withColumn("remote_work", col("remoteWork"))
    .withColumn("grade", col("salaryQualification.title"))
    .withColumn("published_at", to_timestamp(col("publishedDate.date"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
    .withColumn("employer", col("company.title"))
    .withColumn("employment_type", col("employment"))
    .withColumn("salary_from", col("salary.from"))
    .withColumn("salary_to", col("salary.to"))
    .withColumn("salary_currency_id", upper(col("salary.currency")))
    .withColumn("url", functions.concat(lit("https://career.habr.com/vacancies/"), col("id")))


    .withColumn("is_active", not(col("archived")))

  private val vacanciesDF: DataFrame = transformedDF.select("id", "title", "remote_work", "grade", "published_at",
  "employer", "employment_type", "salary_from", "salary_to", "salary_currency_id", "url", "is_active").repartition(conf.transformPartitions)

  private val fieldsDF: DataFrame = transformedDF
    .select(col("id"), explode(col("divisions")).as("fields"))
    .select(col("id"), col("fields.title").as("name"))
    .repartition(1)

  private val skillsDF: DataFrame = transformedDF
    .select(col("id"), explode(col("skills")).as("skill"))
    .select(col("id"), col("skill.title").as("name"))
    .repartition(1)

  private val locationsDF: DataFrame = transformedDF
    .select(col("id"), explode(col("locations")).as("mlocations"))
    .select(col("id"), col("mlocations.title").as("name"))
    .repartition(1)

  HDFSHandler.saveParquet(vacanciesDF, conf.fsConf.getPath(FolderName.Vacancies))
  HDFSHandler.saveParquet(fieldsDF, conf.fsConf.getPath(FolderName.Fields))
  HDFSHandler.saveParquet(skillsDF, conf.fsConf.getPath(FolderName.Skills))
  HDFSHandler.saveParquet(locationsDF, conf.fsConf.getPath(FolderName.Locations))

  spark.stop()
}
