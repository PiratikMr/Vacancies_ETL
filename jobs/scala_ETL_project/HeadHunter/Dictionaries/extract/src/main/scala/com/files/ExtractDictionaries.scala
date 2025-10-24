package com.files

import org.apache.spark.sql.functions.{col, count, explode, when}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.rogach.scallop.ScallopOption

import scala.annotation.tailrec

object ExtractDictionaries extends SparkApp {

  private class Conf(args: Array[String]) extends ProjectConfig(args) {
    private val allDictionaries: String = "areas roles dict:curr dict:schd dict:empl dict:expr"
    val dictionaries: ScallopOption[String] = opt[String](name = "dictionaries", default = Some(allDictionaries))

    verify()
  }

  def main(args: Array[String]): Unit = {

    val conf: Conf = new Conf(args)

    val spark: SparkSession = defineSession(conf.sparkConf)
    val dictSet: Set[String] = conf.dictionaries().split(" ").map(_.trim.toLowerCase).toSet

    import spark.implicits._

    lazy val dictCluster: DataFrame = {
      val response: String = URLHandler.readOrThrow(conf.urlConf, "https://api.hh.ru/dictionaries")
      spark.read.json(Seq(response).toDS)
    }

    dictSet.foreach {
      case "areas" => handleAreas(spark, conf)
      case "roles" => handleRoles(spark, conf)
      case s if s.startsWith("dict:") => handleDictionary(s.stripPrefix("dict:"), dictCluster, conf)
    }

    spark.stop()

  }




  private def handleAreas(spark: SparkSession, conf: Conf): Unit = {

    import spark.implicits._

    val rawDf: DataFrame = {
      val body: String = URLHandler.readOrDefault(conf.urlConf, "https://api.hh.ru/areas")
      spark.read.json(Seq(body).toDS)
    }

    val flatAreas: DataFrame = {
      @tailrec
      def flattenAreas(acc: DataFrame = rawDf.select("id", "name", "parent_id"), current: DataFrame = rawDf): DataFrame = {
        if (current.agg(count(when(functions.size(col("areas")).gt(0), 1))).first().getLong(0) == 0) {
          acc.withColumn("id", col("id").cast(LongType)).withColumn("parent_id", col("parent_id").cast(LongType))
        } else {
          val next = current.withColumn("areas", explode(col("areas"))).select("areas.*").select("id", "name", "parent_id", "areas")
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

  private def handleRoles(spark: SparkSession, conf: Conf): Unit = {

    import spark.implicits._

    val rawDf: DataFrame = {
      val body: String = URLHandler.readOrDefault(conf.urlConf, "https://api.hh.ru/professional_roles")
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

  private def handleDictionary(code: String, cluster: DataFrame, conf: Conf): Unit = {

    def saveData(folderName: FolderName): Unit = {

      val field: String = folderName match {
        case FolderName.Schedule => "schedule"
        case FolderName.Employment => "employment"
        case FolderName.Experience => "experience"
      }

      val df: DataFrame = expl(cluster, field).select("id", "name")
      HDFSHandler.saveParquet(df, conf.fsConf.getPath(folderName))
    }

    code match {
      case "curr" =>
        val curr: DataFrame = expl(cluster,"currency").withColumn("id", col("code")).select("id", "name", "rate")
        HDFSHandler.saveParquet(curr, conf.fsConf.getPath(FolderName.Currency))
      case "schd" => saveData(FolderName.Schedule)
      case "empl" => saveData(FolderName.Employment)
      case "expr" => saveData(FolderName.Experience)
    }

  }

  private def expl(df: DataFrame, field: String): DataFrame = df
    .withColumn(s"$field", explode(col(s"$field"))).select(s"$field.*")
}
