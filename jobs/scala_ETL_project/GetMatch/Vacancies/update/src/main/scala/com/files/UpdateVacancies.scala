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
  private val spark: SparkSession = defineSession(conf.sparkConf)

  private val ids: DataFrame = DBHandler.load(spark, conf.dbConf,
      s"SELECT id FROM gm_vacancies WHERE is_active is true ORDER BY published_at LIMIT ${math.min(conf.limit, conf.activeVacancies())}")
    .repartition(conf.urlConf.requestsPS)


  import spark.implicits._

  private val update: Dataset[Long] = ids.mapPartitions(part => {
    part.flatMap(row => {
      val id = row.getLong(0)
      val body: String = URLHandler.readOrDefault(s"https://getmatch.ru/api/offers/$id", conf.urlConf, """"is_active":true""")
      """\s*"is_active":\s*false\s*""".r.findFirstMatchIn(body) match {
        case Some(_) => Some(id)
        case _ => None
      }
    })
  })

  DBHandler.updateActiveVacancies[Long](conf.dbConf, update)

  spark.stop()

}