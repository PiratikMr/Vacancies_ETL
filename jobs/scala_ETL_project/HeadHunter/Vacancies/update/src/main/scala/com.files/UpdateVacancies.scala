package com.files

import com.files.Common.URLConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.rogach.scallop.ScallopOption
import sttp.model.StatusCode

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


    val vacIds: Dataset[Long] = DBHandler.load(spark, conf.dbConf,
        s"SELECT id FROM hh_vacancies WHERE is_active is true ORDER BY published_at LIMIT ${math.min(conf.activeVacancies(), conf.limit)}")
      .repartition(conf.urlConf.requestsPS).mapPartitions(part => part.map(row => row.getLong(0)))


    val update: Dataset[Long] = vacIds.mapPartitions(part => {

      URLHandler.useClient[Long, Long](part, (backend, id) => {
        val res = URLHandler.read(urlConf, s"https://api.hh.ru/vacancies/$id", Some(backend))
        if (
          (res.isSuccess && res.body.contains(""""archived":true"""))
            || res.code.equals(StatusCode.NotFound)
        ) Some(id)
        else None
      })

    })

    DBHandler.updateActiveVacancies[Long](conf.dbConf, update)

    spark.stop()
  }

}
