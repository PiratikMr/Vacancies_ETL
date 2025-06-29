package com.files

import org.apache.spark.sql.functions.{col, count, explode, when}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.rogach.scallop.ScallopOption

import scala.annotation.tailrec

object ExtractDictionaries extends SparkApp {

  private class Conf(args: Array[String]) extends LocalConfig(args) {

    private val allDictionaries: String = "areas roles dict:curr dict:schd dict:empl dict:expr"
    val dictionaries: ScallopOption[String] = opt[String](name = "dictionaries", default = Some(allDictionaries))

    define()
  }

  def main(args: Array[String]): Unit = {

    val conf: Conf = new Conf(args)
    val spark: SparkSession = defineSession(conf.commonConf)


    val dictSet: Set[String] = conf.dictionaries().split(" ").map(_.trim.toLowerCase).toSet
    val dictionaries: Option[DataFrame] = if (dictSet.exists(_.startsWith("dict:")))
      getDictionariesDF(spark, conf, "https://api.hh.ru/dictionaries")
    else None

    dictSet.foreach(arg => {
      arg.trim.toLowerCase match {
        case "areas" => processAreas(spark, conf, "https://api.hh.ru/areas")
        case "roles" => processRoles(spark, conf, "https://api.hh.ru/professional_roles")
        case s if s.startsWith("dict:") =>
          dictionaries match {
            case Some(data) => processDictionary(conf, data, s.stripPrefix("dict:"))
            case None => ()
          }
      }
    })


    spark.stop()

  }



  private def areasToDF(spark: SparkSession, body: String): DataFrame = {
    val initialDF = toDF(spark, body).drop("areas")

    @tailrec
    def flattenAreas(acc: DataFrame = initialDF, current: DataFrame = toDF(spark, body)): DataFrame = {
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

  private def processAreas(spark: SparkSession, conf: LocalConfig, url: String): Unit = {
    import spark.implicits._

    val areas: String = URLHandler.readURL(url, conf.commonConf) match {
      case Some(body) => body
      case None => ""
    }

    val areasDF: DataFrame = areasToDF(spark, areas)

    val areasMap: Map[Long,(String, Option[Long])] = areasDF.rdd
      .map(row => {
        val id: Long = row.getAs[Long]("id")
        val name: String = row.getAs[String]("name")
        val parent_id: Option[Long] = Option(row.getAs[Long]("parent_id"))

        (id, (name, parent_id))
      })
      .collectAsMap()
      .toMap

    val transformedAreasDF: DataFrame = areasMap.map { case (i, (name, p)) =>
        @tailrec
        def loop(id: Long, prev: Option[Long], parent: Option[Long]): (Long, Option[Long]) = {
          parent match {
            case Some(value) => loop(id, Some(value), areasMap(value)._2)
            case None => (id, prev)
          }
        }

        val (id, country_id) = loop(i, p, p)
        (id, name, country_id)

      }.toSeq
      .toDF("id", "name", "parent_id")

    HDFSHandler.save(conf.commonConf)(FolderName.Areas, transformedAreasDF)

  }


  private def processRoles(spark: SparkSession, conf: LocalConfig, url: String): Unit = {

    val roles: String = URLHandler.readURL(url, conf.commonConf) match {
      case Some(body) => body
      case None => ""
    }

    val rolesDF: DataFrame = toDF(spark, roles)
      .withColumn("categories", explode(col("categories")))
      .select("categories.*")
      .withColumn("id", col("id").cast(LongType))
      .withColumn("roles", explode(col("roles")))
      .select(col("id").as("parent_id"),
        col("roles").getField("id").cast(LongType).as("id"),
        col("roles").getField("name").as("name"))

    HDFSHandler.save(conf.commonConf)(FolderName.Roles, rolesDF)

  }


  private def getDictionariesDF(spark: SparkSession, conf: LocalConfig, url: String): Option[DataFrame] = {
    URLHandler.readURL(url, conf.commonConf) match {
      case Some(body) => Some(toDF(spark, body))
      case None => None
    }
  }


  private def dictHelper(data:DataFrame)(name: String): DataFrame = expl(data, name).select("id", "name")

  private def processDictionary(conf: Conf, data: DataFrame, code: String): Unit = {

    val saveWithConf = HDFSHandler.save(conf.commonConf)_
    val defData = dictHelper(data)_

    code match {
      case "curr" => saveWithConf(
        FolderName.Currency,
        expl(data,"currency")
          .withColumn("id", col("code"))
          .select("id", "name", "rate")
      )
      case "schd" => saveWithConf(
        FolderName.Schedule,
        defData("schedule")
      )
      case "empl" => saveWithConf(
        FolderName.Employment,
        defData("employment")
      )
      case "expr" => saveWithConf(
        FolderName.Experience,
        defData("experience")
      )
    }
  }


  private def expl(df: DataFrame, field: String): DataFrame = df
    .withColumn(s"$field", explode(col(s"$field"))).select(s"$field.*")

  private def toDF(spark: SparkSession, s: String): DataFrame = {
    import spark.implicits._
    spark.read.json(Seq(s).toDS())
  }
}
