package com.files

import com.files.Common.URLConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object ExtractVacancies extends SparkApp {

  private def vacanciesUrl(perPage: Int, page: Int): String =
    s"https://career.habr.com/api/frontend/vacancies?sort=date&type=all&per_page=$perPage&page=$page"

  def main(args: Array[String]): Unit = {

    val conf = new ProjectConfig(args) {
      lazy val vacsPageLimit: Int = math.max(1, getFromConfFile[Int]("vacsPageLimit"))
      lazy val vacsPerPage: Int = getFromConfFile[Int]("vacsPerPage")
      lazy val rawPartitions: Int = getFromConfFile[Int]("rawPartitions")

      verify()
    }

    val spark: SparkSession = defineSession(conf.sparkConf)
    import spark.implicits._

    // parameters
    val urlConf: URLConf = conf.urlConf

    val totalVacs: Int = math.min(conf.vacsPageLimit * conf.vacsPerPage, {
      val body: String = URLHandler.readOrDefault(conf.urlConf, vacanciesUrl(1, 0), """"totalPages":0""")
      """"totalPages"\s*:\s*(\d+)""".r.findFirstMatchIn(body).get.group(1).toInt
    })


    val urlsDS: Dataset[String] = (0 to (totalVacs / conf.vacsPerPage))
      .map(page => vacanciesUrl(conf.vacsPerPage, page)).toDS().repartition(conf.urlConf.requestsPS)

    val vacanciesDS: Dataset[String] = urlsDS.mapPartitions(part => {
      URLHandler.useClient[String, String](part, (backend, url) => {
        URLHandler.readOrNone(urlConf, url, Some(backend))
      })
    }).repartition(conf.rawPartitions)

    vacanciesDS.write.mode(SaveMode.Overwrite).text(conf.fsConf.getPath(FolderName.RawVacancies))

    spark.stop()
  }

}
