package com.files

import org.apache.spark.sql.functions.{col, count, explode, when}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.rogach.scallop.ScallopOption

import scala.annotation.tailrec

object ExtractDictionaries extends App with SparkApp {

  private class Conf(args: Array[String]) extends LocalConfig(args) {
    private val allDictionaries: String = "areas roles dict:curr dict:schd dict:empl dict:expr"
    val dictionaries: ScallopOption[String] = opt[String](name = "dictionaries", default = Some(allDictionaries))

    define()
  }

  private val conf: Conf = new Conf(args)
  private val spark: SparkSession = defineSession(conf.sparkConf)
  private val dictSet: Set[String] = conf.dictionaries().split(" ").map(_.trim.toLowerCase).toSet


  import spark.implicits._

  private lazy val dictCluster: DataFrame = {
    val response: String = URLHandler.readOrThrow("https://api.hh.ru/dictionaries", conf.urlConf)
    spark.read.json(Seq(response).toDS)
  }


  dictSet.foreach {
    case "areas" => handleAreas()
    case "roles" => handleRoles()
    case s if s.startsWith("dict:") => handleDictionary(s.stripPrefix("dict:"))
  }

  spark.stop()


  private def handleAreas(): Unit = {

    val rawDf: DataFrame = {
      val body: String = URLHandler.readOrDefault("https://api.hh.ru/areas", conf.urlConf, "")
      spark.read.json(Seq(body).toDS)
    }

    val flatAreas: DataFrame = {
      @tailrec
      def flattenAreas(acc: DataFrame = rawDf.drop("areas"), current: DataFrame = rawDf): DataFrame = {
        if (current.agg(count(when(functions.size(col("areas")).gt(0), 1))).first().getLong(0) == 0) {
          acc.withColumn("id", col("id").cast(LongType)).withColumn("parent_id", col("parent_id").cast(LongType))
        } else {
          val next = current.withColumn("areas", explode(col("areas"))).select("areas.*")
          flattenAreas(acc.union(next.drop("areas")), next)
        }
      }
      flattenAreas()
    }

    val areasMap: Map[Long,(String, Option[Long])] = flatAreas.rdd
      .map(row => {
        val id: Long = row.getAs[Long]("id")
        val name: String = row.getAs[String]("name")
        val parent_id: Option[Long] = Option(row.getAs[Long]("parent_id"))

        (id, (name, parent_id))
      })
      .collectAsMap()
      .toMap

    val areas: DataFrame = areasMap.map { case (i, (name, p)) =>
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

    HDFSHandler.saveParquet(areas, conf.fsConf.getPath(FolderName.Areas))
  }

  private def handleRoles(): Unit = {

    val rawDf: DataFrame = {
      val body: String = URLHandler.readOrDefault("https://api.hh.ru/professional_roles", conf.urlConf, "")
      spark.read.json(Seq(body).toDS)
    }


    val roles: DataFrame = rawDf
      .withColumn("categories", explode(col("categories"))).select("categories.*")
      .withColumn("id", col("id").cast(LongType))
      .withColumn("roles", explode(col("roles")))
      .select(col("id").as("parent_id"),
        col("roles.id").cast(LongType).as("id"),
        col("roles.name").as("name"))

    HDFSHandler.saveParquet(roles, conf.fsConf.getPath(FolderName.Roles))
  }

  private def handleDictionary(code: String): Unit = {

    def saveData(folderName: FolderName): Unit = {

      val field: String = folderName match {
        case FolderName.Schedule => "schedule"
        case FolderName.Employment => "employment"
        case FolderName.Experience => "experience"
      }

      val df: DataFrame = expl(dictCluster, field).select("id", "name")
      HDFSHandler.saveParquet(df, conf.fsConf.getPath(folderName))
    }

    code match {
      case "curr" =>
        val curr: DataFrame = expl(dictCluster,"currency").withColumn("id", col("code")).select("id", "name", "rate")
        HDFSHandler.saveParquet(curr, conf.fsConf.getPath(FolderName.Currency))
      case "schd" => saveData(FolderName.Schedule)
      case "empl" => saveData(FolderName.Employment)
      case "expr" => saveData(FolderName.Experience)
    }

  }
  private def expl(df: DataFrame, field: String): DataFrame = df
    .withColumn(s"$field", explode(col(s"$field"))).select(s"$field.*")
}
