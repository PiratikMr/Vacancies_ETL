package com.files

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.rogach.scallop.ScallopOption

object UpdateVacancies extends App with SparkApp {

  private class Conf(args: Array[String]) extends LocalConfig(args) {
    lazy val activeVacancies: ScallopOption[Int] = opt[Int](name = "activevacancies")
    lazy val limit: Int = getFromConfFile[Int]("updateLimit")

    define()
  }

  private val conf: Conf = new Conf(args)
  private val spark: SparkSession = defineSession(conf.sparkConf, conf.urlConf.requestsPS)

  private val activeIds: DataFrame = DBHandler.load(spark, conf.dbConf,
      s"SELECT id FROM fn_vacancies WHERE is_active is true ORDER BY published_at LIMIT ${math.min(conf.limit, conf.activeVacancies())}")
    .repartition(conf.urlConf.requestsPS)

  import spark.implicits._

  private val update: Dataset[Long] = activeIds.mapPartitions(part => part.flatMap(row => {
    val id: Long = row.getLong(0)
    val body: String = URLHandler.readOrDefault(s"https://api.finder.work/api/v2/vacancies/$id", conf.urlConf, """{"status": "active"}""")
    """\s*"status": "active"\s*""".r.findFirstMatchIn(body) match {
      case Some(_) => None
      case None => Some(id)
    }
  }))

  DBHandler.updateActiveVacancies[Long](conf.dbConf, update)

  spark.stop()
}