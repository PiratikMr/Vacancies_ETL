package com.files

import com.files.Common.URLConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.rogach.scallop.ScallopOption

object UpdateVacancies extends SparkApp {

  def main(args: Array[String]): Unit = {

    val conf = new ProjectConfig(args) {
      lazy val activeVacancies: ScallopOption[Int] = opt[Int](name = "activevacancies")
      lazy val limit: Int = getFromConfFile[Int]("updateLimit")

      verify()
    }

    val spark: SparkSession = defineSession(conf.sparkConf)
    import spark.implicits._

    // parameters
    val urlConf: URLConf = conf.urlConf

    val ids: Dataset[String] = DBHandler.load(spark, conf.dbConf,
        s"SELECT id FROM gj_vacancies WHERE is_active is true ORDER BY published_at LIMIT ${math.min(conf.limit, conf.activeVacancies())}")
      .repartition(urlConf.requestsPS).mapPartitions(part => part.map(row => row.getString(0)))


    val update: Dataset[String] = ids.mapPartitions(part => {

      URLHandler.useClient[String, String](part, (backend, id) => {
        URLHandler.readOrNone(urlConf, s"https://geekjob.ru/vacancy/$id", Some(backend)) match {
          case Some(body) if body.contains("Эта вакансия была перемещена в архив.") => Some(id)
          case _ => None
        }
      })

    })

    DBHandler.updateActiveVacancies[String](conf.dbConf, update)

    spark.stop()

  }


}
