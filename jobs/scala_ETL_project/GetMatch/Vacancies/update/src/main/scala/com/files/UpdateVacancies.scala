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



    val ids: Dataset[Long] = DBHandler.load(spark, conf.dbConf,
        s"SELECT id FROM gm_vacancies WHERE is_active is true ORDER BY published_at LIMIT ${math.min(conf.limit, conf.activeVacancies())}")
      .repartition(conf.urlConf.requestsPS).mapPartitions(part => part.map(row => row.getLong(0)))

    val update: Dataset[Long] = ids.mapPartitions(part => {

      URLHandler.useClient[Long, Long](part, (backend, id) => {
        val body: String = URLHandler.readOrDefault(urlConf,  s"https://getmatch.ru/api/offers/$id", """"is_active":true""", Some(backend))
        """\s*"is_active":\s*false\s*""".r.findFirstMatchIn(body) match {
          case Some(_) => Some(id)
          case _ => None
        }
      })
    })

    DBHandler.updateActiveVacancies[Long](conf.dbConf, update)

    spark.stop()

  }

}