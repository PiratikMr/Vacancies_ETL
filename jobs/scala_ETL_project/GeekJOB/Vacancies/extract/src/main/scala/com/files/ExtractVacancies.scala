package com.files

import com.files.Common.URLConf
import com.files.URLHandler._
import org.apache.spark.SparkJobInfo
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object ExtractVacancies extends SparkApp {

  private def pageURL(page: Int): String = s"https://geekjob.ru/vacancies/$page"
  private def vacURL(code: String): String = s"https://geekjob.ru/vacancy/$code"

  def main(args: Array[String]): Unit = {

    val conf = new ProjectConfig(args) {
      lazy val pageLimit: Int = getFromConfFile[Int]("pageLimit")
      lazy val rawPartitions: Int = getFromConfFile[Int]("rawPartitions")

      verify()
    }

    val spark: SparkSession = defineSession(conf.sparkConf)
    import spark.implicits._

    // parameters
    val urlConf: URLConf = conf.urlConf



    val firstPageContent: String = URLHandler.readOrThrow(urlConf, pageURL(1))

    val pageURLs: Dataset[String] = {
      val totalPages: Int = """<small>страниц (\d+)</small>""".r.findFirstMatchIn(firstPageContent).get.group(1).toInt
      val pagesToProcess: Int = math.min(totalPages, conf.pageLimit)

      (2 to pagesToProcess).map(page => pageURL(page))
    }.toDS

    val vacancyIds: Dataset[String] = pageURLs.repartition(urlConf.requestsPS).mapPartitions(part => {

      URLHandler.useClient[String, String](part, (backend, url) => {
        URLHandler.readOrNone(urlConf, url, Some(backend)) match {
          case Some(body) => """/vacancy/([a-z0-9]{24})""".r.findAllMatchIn(body).map(_.group(1))
          case None => None
        }
      })

    }).dropDuplicates()

    val vacanciesDS: Dataset[String] = vacancyIds.repartition(urlConf.requestsPS).mapPartitions(part => {

      URLHandler.useClient[String, String](part, (backend, id) => {
        readOrNone(urlConf, vacURL(id), Some(backend)) match {
          case Some(body) =>
            val start: Int = body.indexOf("""<article class="row vacancy">""")

            val bodyEnd: String = "</article>"
            val end: Int = body.indexOf(bodyEnd, start) + bodyEnd.length

            Some(id + body.substring(start, end).replace("\n", ""))
          case None => None
        }
      })
    })

    vacanciesDS.repartition(conf.rawPartitions).write.mode(SaveMode.Overwrite).text(conf.fsConf.getPath(FolderName.RawVacancies))

    spark.stop()

  }

}