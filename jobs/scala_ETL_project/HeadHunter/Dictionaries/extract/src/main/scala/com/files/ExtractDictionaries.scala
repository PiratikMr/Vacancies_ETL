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
  override val ss: SparkSession = defineSession(conf.fileConf)

  // areas
  private val areasDF: DataFrame = toDF(takeURL("https://api.hh.ru/areas", conf.fileConf).get)
  private val areasTDF: DataFrame = {
    @tailrec
    def f(acc: DataFrame = areasDF.drop("areas"), i: DataFrame = areasDF): DataFrame = {
      if (i.agg(count(when(functions.size(col("areas")).gt(0), 1))).first().getLong(0) == 0) {
        acc.
          withColumn("id", col("id").cast(LongType)).
          withColumn("parent_id", col("parent_id").cast(LongType))
      }
      else {
        val next: DataFrame = i.withColumn("areas", explode(col("areas"))).select("areas.*")
        f(acc.union(next.drop("areas")),next)
      }
    }
    f()
  }

  // roles
  private val rolesDF: DataFrame = toDF(takeURL("https://api.hh.ru/professional_roles", conf.fileConf).get)
  private val rolesTDF: DataFrame = rolesDF
      .withColumn("categories", explode(col("categories")))
      .select("categories.*")
      .withColumn("id", col("id").cast(LongType))
      .withColumn("roles", explode(col("roles")))
      .select(col("id").as("parent_id"),
        col("roles").getField("id").cast(LongType).as("id"),
        col("roles").getField("name").as("name"))

  // dictionaries
  private val dictionariesDF: DataFrame = toDF(takeURL("https://api.hh.ru/dictionaries", conf.fileConf).get)

  save(FolderName.Areas, areasTDF)

  save(FolderName.Roles, rolesTDF)

  save(FolderName.Currency, expl(dictionariesDF,"currency")
    .withColumn("id", col("code"))
    .select("id", "name", "rate"))

  save(FolderName.Schedule, expl(dictionariesDF,"schedule")
    .select("id", "name"))

  save(FolderName.Employment, expl(dictionariesDF,"employment")
    .select("id", "name"))

  save(FolderName.Experience, expl(dictionariesDF,"experience")
    .select("id", "name"))

  stopSpark()


  private def save(folderName: FolderName, data: DataFrame): Unit = {
    give(
      conf = conf.fileConf,
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
