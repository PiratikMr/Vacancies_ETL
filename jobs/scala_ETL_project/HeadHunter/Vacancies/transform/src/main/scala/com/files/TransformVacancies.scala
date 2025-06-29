package com.files

import com.files.FolderName.FolderName
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

object TransformVacancies extends SparkApp {

  private class Conf(args: Array[String]) extends LocalConfig(args) {
    lazy val transformPartitions: Int = getFromConfFile[Int]("transformPartitions")

    define()
  }

  def main(args: Array[String]): Unit = {

    val conf: Conf = new Conf(args)
    val spark: SparkSession = defineSession(conf.commonConf)

    val rawData: DataFrame = HDFSHandler.load(spark, conf.commonConf)(FolderName.Raw)
    val areas: DataFrame = getAreasDF(spark, conf)

    val transformedData: DataFrame = transformData(rawData, areas)

    saveData(conf, transformedData)

    spark.stop()

  }


  private def getAreasDF(spark: SparkSession, conf: Conf): DataFrame = {
    HDFSHandler.load(spark, conf.commonConf)(FolderName.Areas)
      .select(col("id").as("area_id"), col("parent_id").as("country_area_id"))
  }


  private def transformData(rawData: DataFrame, areas: DataFrame): DataFrame = rawData
      .withColumn("id", col("id").cast(LongType))

      .withColumn("region_area_id", col("area").getField("id").cast(LongType))
      .join(areas, col("region_area_id") === col("area_id"), "left")
      .withColumn("close_to_metro", col("address").getField("metro_stations").isNotNull)

      .withColumn("salary_from", col("salary").getField("from"))
      .withColumn("salary_to", col("salary").getField("to"))
      .withColumn("currency_id", col("salary").getField("currency"))

      .withColumn("employer_id", col("employer").getField("id").cast(LongType))
      .withColumn("employer_name", col("employer").getField("name"))

      .withColumn("skills",
        if (rawData.columns.contains("key_skills")) { col("key_skills") }
        else { lit(Array.empty[String]) }
      )

      .withColumn("schedule_id", col("schedule").getField("id"))
      .withColumn("experience_id", col("experience").getField("id"))
      .withColumn("employment_id", col("employment").getField("id"))

      .withColumn("publish_date", to_timestamp(col("published_at"), "yyyy-MM-dd'T'HH:mm:ssZ"))

      .withColumn("roles", explode(col("professional_roles")))
      .withColumn("role_id", col("roles.id").cast(LongType))

      .dropDuplicates("id")


  private def saveData(conf: Conf, data: DataFrame): Unit = {

    val vacancies: DataFrame = data
      .select("id", "name", "region_area_id", "country_area_id", "salary_from", "salary_to", "close_to_metro", "publish_date",
        "schedule_id", "experience_id", "employment_id", "employer_id", "currency_id", "role_id")

    val employers: DataFrame = data
      .select(col("employer_id").as("id"), col("employer_name").as("name"))
      .filter(col("id").isNotNull)
      .dropDuplicates("id")

    val skills: DataFrame = data
      .select(col("id"), explode(col("skills")).as("name"))


    val saveCurr = save(conf.commonConf)_

    saveCurr(FolderName.Vac, vacancies, conf.transformPartitions)
    saveCurr(FolderName.Employer, employers, 1)
    saveCurr(FolderName.Skills, skills, 1)

  }


  private def save (conf: CommonConfig)(folderName: FolderName, data: DataFrame, repartition: Int): Unit = {
    HDFSHandler.save(conf)(folderName, data.repartition(repartition))
  }
}