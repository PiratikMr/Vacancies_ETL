package com.files

import EL.Load.give
import Spark.SparkApp
import com.Config.FolderName.FolderName
import com.Config.{FolderName, LocalConfig}
import com.extractURL.ExtractURL.takeURL
import org.apache.spark.sql.functions.{col, count, explode, when}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.annotation.tailrec

object ExtractDictionaries extends App with SparkApp {

  private val conf = new LocalConfig(args) {
    define()
  }
  override val ss: SparkSession = defineSession(conf.commonConf)

  // areas
  private val areasDFOpt: Option[DataFrame] = for {
    body <- takeURL("https://api.hh.ru/areas", conf.commonConf)
  } yield processAreas(body)
  areasDFOpt.foreach(save(FolderName.Areas, _))

  // roles
  private val rolesDFOpt: Option[DataFrame] = for {
    body <- takeURL("https://api.hh.ru/professional_roles", conf.commonConf)
  } yield processRoles(body)
  rolesDFOpt.foreach(save(FolderName.Roles, _))

  // dictionaries
  private val dictionariesDFOpt: Option[DataFrame] = takeURL("https://api.hh.ru/professional_roles", conf.commonConf).map(toDF)

  dictionariesDFOpt.foreach { df =>
    save(FolderName.Currency, expl(df,"currency")
      .withColumn("id", col("code"))
      .select("id", "name", "rate"))
    save(FolderName.Schedule, expl(df,"schedule")
      .select("id", "name"))
    save(FolderName.Employment, expl(df,"employment")
      .select("id", "name"))
    save(FolderName.Experience, expl(df,"experience")
      .select("id", "name"))
  }

  stopSpark()



  private def processAreas(body: String): DataFrame = {
    val initialDF = toDF(body).drop("areas")

    @tailrec
    def flattenAreas(acc: DataFrame = initialDF, current: DataFrame = toDF(body)): DataFrame = {
      if (current.agg(count(when(functions.size(col("areas")).gt(0), 1))).first().getLong(0) == 0) {
        acc
          .withColumn("id", col("id").cast(LongType))
          .withColumn("parent_id", col("parent_id").cast(LongType))
      } else {
        val next = current.withColumn("areas", explode(col("areas"))).select("areas.*")
        flattenAreas(acc.union(next.drop("areas")), next)
      }
    }

    flattenAreas()
  }

  private def processRoles(body: String): DataFrame = {
    toDF(body)
      .withColumn("categories", explode(col("categories")))
      .select("categories.*")
      .withColumn("id", col("id").cast(LongType))
      .withColumn("roles", explode(col("roles")))
      .select(col("id").as("parent_id"),
        col("roles").getField("id").cast(LongType).as("id"),
        col("roles").getField("name").as("name"))
  }


  private def save(folderName: FolderName, data: DataFrame): Unit = {
    give(
      conf = conf.commonConf,
      folderName = FolderName.Dict(folderName),
      data = data
    )
  }
  private def expl(df: DataFrame, field: String): DataFrame = df
    .withColumn(s"$field", explode(col(s"$field"))).select(s"$field.*")
  private def toDF(s: String): DataFrame = {
    import ss.implicits._
    ss.read.json(Seq(s).toDS())
  }
}
