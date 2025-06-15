package com.files

import Spark.SparkApp
import com.Config.{FolderName, LocalConfig}
import com.extractURL.ExtractURL.{requestError, takeURL}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex

object ExtractVacancies extends App with SparkApp {

  private val conf = new LocalConfig(args) {
    lazy val pageLimit: Int = getFromConfFile[Int]("pageLimit")
    lazy val rawPartitions: Int = getFromConfFile[Int]("rawPartitions")

    define()
  }
  override val ss: SparkSession = defineSession(conf.commonConf)

  private val pagesPattern: Regex = """<small>страниц (\d+)</small>""".r
  private val codePattern: Regex = """/vacancy/([a-z0-9]{24})"""".r

  private val codesRDD: RDD[String] = {
    val firstPageContent = takeURL(pageUrl(1), conf.commonConf) match {
      case Some(body) => body
      case _ => requestError(pageUrl(1))
    }
    val totalPages = pagesPattern.findFirstMatchIn(firstPageContent).get.group(1).toInt
    val pagesToProcess = math.min(totalPages, conf.pageLimit)

    val pagesRDD = ss.sparkContext.parallelize(2 to pagesToProcess, conf.rawPartitions)

    val codesRDD = pagesRDD.flatMap { page =>
      takeURL(pageUrl(page), conf.commonConf) match {
        case Some(content) => codePattern.findAllMatchIn(content).map(_.group(1)).toSeq.distinct
        case _ => Seq.empty[String]
      }
    }.distinct()

    val firstPageCodes = ss.sparkContext.parallelize(
      codePattern.findAllMatchIn(firstPageContent).map(_.group(1)).toSeq.distinct
    )

    firstPageCodes.union(codesRDD).distinct()
  }

  println(s"Total codes: ${codesRDD.count()}")

  private val vacanciesRDD: RDD[String] = {
    codesRDD.flatMap { code =>
      takeURL(vacancyUrl(code), conf.commonConf) match {
        case Some(content) =>
          val endText: String = "</article>"

          val fullText: String = content
          val startIdx: Integer = fullText.indexOf("""<article class="row vacancy">""")
          val endIdx: Integer = fullText.indexOf(endText, startIdx) + endText.length

          Some(code + fullText.substring(startIdx, endIdx).replace("\n", ""))
        case _ => None
      }
    }
  }

  println(s"Total vacancies loaded: ${vacanciesRDD.count()}")

  private val outputPath = conf.commonConf.fs.getPath(FolderName.Raw)
  ss.sparkContext.hadoopConfiguration.set("fs.defaultFS", conf.commonConf.fs.url)
  private val fs = FileSystem.get(ss.sparkContext.hadoopConfiguration)

  if (fs.exists(new Path(outputPath))) {
    fs.delete(new Path(outputPath), true)
  }

  vacanciesRDD.repartition(conf.rawPartitions).saveAsTextFile(outputPath)


  stopSpark()

  private def pageUrl(page: Integer): String = {
    s"https://geekjob.ru/vacancies/$page"
  }

  private def vacancyUrl(code: String): String = {
    s"https://geekjob.ru/vacancy/$code"
  }
}
