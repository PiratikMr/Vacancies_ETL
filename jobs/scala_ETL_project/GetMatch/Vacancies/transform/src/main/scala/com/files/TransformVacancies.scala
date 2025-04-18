package com.files

import EL.Extract.take
import EL.Load.give
import Spark.SparkApp
import com.Config.FolderName.FolderName
import com.Config.{FolderName, LocalConfig}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.ScallopOption

object TransformVacancies extends App with SparkApp {

  private val conf = new LocalConfig(args) {
    val partitions: ScallopOption[Int] = opt[Int](default = Some(1), validate = _ > 0)

    define()
  }

  override val ss: SparkSession = defineSession(conf.fileConf)

  private val rawVac: DataFrame = take(
    ss = ss,
    conf = conf.fileConf,
    folderName = FolderName.Raw
  ).get

  private val genVac: DataFrame = rawVac
    .withColumn("id", col("id").cast(LongType))
    .withColumn("name", col("position"))

    .withColumn("publish_date", to_timestamp(col("published_at"), "yyyy-MM-dd"))

    .withColumn("salary_from", col("salary_display_from"))
    .withColumn("salary_to", col("salary_display_to"))
    .withColumn("currency", col("salary_currency"))

    .withColumn("skills", col("stack"))
    .withColumn("locations", col("display_locations"))

    .withColumn("english_lvl", col("english_level").getField("name"))
    .withColumn("remote_op", col("remote_options"))
    .withColumn("office_op", col("office_options"))
    .withColumn("employer", col("company").getField("name"))

    .dropDuplicates("id")

  private val transformVac: DataFrame = genVac
    .select("id", "name", "publish_date", "salary_from", "salary_to", "salary_hidden", "currency",
    "english_lvl", "remote_op", "office_op", "employer")

  private val locations: DataFrame = genVac
    .select(col("id"), explode(col("locations")).as("location"))
    .select(col("id"), col("location.city").as("city"), col("location.country").as("country"))

  private val skills: DataFrame = genVac
    .select(col("id"), explode(col("skills")).as("name"))

  // skills
  save(FolderName.Skills, skills)

  // locations
  save(FolderName.Locations, locations)

  // vacancies
  save(FolderName.Vac, transformVac, conf.partitions())

  stopSpark()

  private def save(folderName: FolderName, dataFrame: DataFrame, repartition: Integer = 1): Unit = {
    give(
      conf = conf.fileConf,
      data = dataFrame.repartition(repartition),
      folderName = folderName
    )
  }
}
