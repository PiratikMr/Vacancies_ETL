package com.files

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


    val currency: DataFrame = DBHandler.load(spark, conf.commonConf, conf.tableName(FolderName.Currency))
      .select(col("id").as("c_id"), col("code").as("c_code"))

    val rawData: DataFrame = HDFSHandler.load(spark, conf.commonConf)(FolderName.Raw)


    val transformedData: DataFrame = transformData(rawData, currency)

    saveData(conf, transformedData)

    spark.stop

  }


  private def transformData(data: DataFrame, currency: DataFrame): DataFrame =
    data
    .join(currency, data("salary_currency") === currency("c_code"), "left_outer")

    .withColumn("id", col("id").cast(LongType))
    .withColumn("name", col("position"))

    .withColumn("publish_date", to_timestamp(col("published_at"), "yyyy-MM-dd"))

    .withColumn("salary_from", col("salary_display_from"))
    .withColumn("salary_to", col("salary_display_to"))
    .withColumn("currency_id",
      when(col("c_code").isNotNull, col("c_id"))
        .otherwise(col("salary_currency")))

    .withColumn("skills", col("stack"))
    .withColumn("locations", col("display_locations"))

    .withColumn("english_lvl", col("english_level").getField("name"))
    .withColumn("remote_op", col("remote_options"))
    .withColumn("office_op", col("office_options"))
    .withColumn("employer", col("company").getField("name"))

    .dropDuplicates("id")


  private def saveData(conf: Conf, data: DataFrame): Unit = {

    val vacancies: DataFrame = data
      .select("id", "name", "publish_date", "salary_from", "salary_to", "salary_hidden", "currency_id",
        "english_lvl", "remote_op", "office_op", "employer")

    val locations: DataFrame = data
      .select(col("id"), explode(col("locations")).as("location"))
      .select(col("id"), col("location.city").as("city"), col("location.country").as("country"))

    val skills: DataFrame = data.
      select(col("id"), explode(col("skills")).as("name"))


    val saveWithConf = HDFSHandler.save(conf.commonConf)_

    saveWithConf(FolderName.Vac, vacancies.repartition(conf.transformPartitions))
    saveWithConf(FolderName.Locations, locations.repartition(1))
    saveWithConf(FolderName.Skills, skills.repartition(1))

  }

}
