package com.files

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.rogach.scallop.ScallopOption
import sttp.model.StatusCode

object UpdateVacancies extends App with SparkApp {

  private class Conf(args: Array[String]) extends LocalConfig(args) {
    lazy val activeVacancies: ScallopOption[Int] = opt[Int](name = "activevacancies")
    lazy val limit: Int = getFromConfFile[Int]("updateLimit")

    define()
  }

  private val conf: Conf = new Conf(args)
  private val spark: SparkSession = defineSession(conf.sparkConf, conf.urlConf.requestsPS)


  private val ids: DataFrame = DBHandler.load(spark, conf.dbConf,
    s"SELECT id FROM hh_vacancies WHERE is_active is true ORDER BY published_at LIMIT ${math.min(conf.activeVacancies(), conf.limit)}")
    .repartition(conf.urlConf.requestsPS)


  import spark.implicits._

  private val update: Dataset[Long] = ids.mapPartitions(part => {
    part.flatMap(row => {
      val id: Long = row.getLong(0)
      val res = URLHandler.read(s"https://api.hh.ru/vacancies/$id", conf.urlConf)

      if (
        (res.isSuccess && res.body.right.get.contains(""""archived":true"""))
          || res.code.equals(StatusCode.NotFound)
      ) Some(id)
      else None
    })
  })

  DBHandler.updateActiveVacancies[Long](conf.dbConf, update)

  spark.stop()
}
