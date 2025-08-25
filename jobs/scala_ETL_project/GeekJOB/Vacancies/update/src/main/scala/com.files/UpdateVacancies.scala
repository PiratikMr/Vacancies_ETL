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


  private val ids: DataFrame = DBHandler.load(spark, conf.dbConf,
    s"SELECT id FROM gj_vacancies WHERE is_active is true ORDER BY published_at LIMIT ${math.min(conf.activeVacancies(), conf.limit)}")
    .repartition(conf.urlConf.requestsPS)


  import spark.implicits._

  private val update: Dataset[String] = ids.mapPartitions(part => {
    part.flatMap(row => {
      val id = row.getString(0)
      URLHandler.readOrNone(s"https://geekjob.ru/vacancy/$id", conf.urlConf) match {
        case Some(body) if body.contains("Эта вакансия была перемещена в архив.") => Some(id)
        case _ => None
      }
    })
  })

  DBHandler.updateActiveVacancies[String](conf.dbConf, update)

  spark.stop()

}
