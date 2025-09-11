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


    val activeIds: DataFrame = DBHandler.load(spark, conf.dbConf,
        s"SELECT id FROM fn_vacancies WHERE is_active is true ORDER BY published_at LIMIT ${math.min(conf.limit, conf.activeVacancies())}")
      .repartition(conf.urlConf.requestsPS)


    val update: Dataset[Long] = activeIds.mapPartitions(part => part.flatMap(row => {
      val id: Long = row.getLong(0)
      val body: String = URLHandler.readOrDefault(urlConf, s"https://api.finder.work/api/v2/vacancies/$id", """{"status": "pupu"}""")
      """\s*"status": "active"\s*""".r.findFirstMatchIn(body) match {
        case Some(_) => None
        case None => Some(id)
      }
    }))

    DBHandler.updateActiveVacancies[Long](conf.dbConf, update)

    spark.stop()

  }

}