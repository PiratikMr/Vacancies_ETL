package com.files

import com.files.URLHandler._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object ExtractVacancies extends App with SparkApp {

  private class Conf(args: Array[String]) extends LocalConfig(args) {
    lazy val pageLimit: Int = getFromConfFile[Int]("pageLimit")
    lazy val rawPartitions: Int = getFromConfFile[Int]("rawPartitions")

    define()
  }

  private val conf: Conf = new Conf(args)
  private val spark: SparkSession = defineSession(conf.sparkConf, conf.urlConf.requestsPS)


  private def pageURL(page: Int): String = s"https://geekjob.ru/vacancies/$page"
  private def vacURL(code: String): String = s"https://geekjob.ru/vacancy/$code"


  private val firstPageContent: String = readOrThrow(pageURL(1), conf.urlConf)


  import spark.implicits._

  private val pageURLs: Dataset[String] = {
    val totalPages: Int = """<small>страниц (\d+)</small>""".r.findFirstMatchIn(firstPageContent).get.group(1).toInt
    val pagesToProcess: Int = math.min(totalPages, conf.pageLimit)

    (2 to pagesToProcess).map(page => pageURL(page))
  }.toDS

  private val vacancyIds: Dataset[String] = pageURLs.repartition(conf.urlConf.requestsPS).mapPartitions(part => {
    part.flatMap(url => {
      readOrNone(url, conf.urlConf) match {
        case Some(body) => """/vacancy/([a-z0-9]{24})""".r.findAllMatchIn(body).map(_.group(1))
        case None => None
      }
    })
  }).dropDuplicates()

  private val vacanciesDS: Dataset[String] = vacancyIds.repartition(conf.urlConf.requestsPS).mapPartitions(part => {
    part.flatMap(id => {
      readOrNone(vacURL(id), conf.urlConf) match {
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