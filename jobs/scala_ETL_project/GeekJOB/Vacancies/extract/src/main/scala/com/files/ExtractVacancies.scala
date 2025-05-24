package com.files

import Spark.SparkApp
import com.Config.{FolderName, LocalConfig}
import com.extractURL.ExtractURL.takeURL
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex
import scala.util.{Failure, Success}

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
    val firstPageContent = takeURL(pageUrl(1), conf.commonConf).get
    val totalPages = pagesPattern.findFirstMatchIn(firstPageContent).get.group(1).toInt
    val pagesToProcess = math.min(totalPages, conf.pageLimit)

    val pagesRDD = ss.sparkContext.parallelize(2 to pagesToProcess, conf.rawPartitions)

    val codesRDD = pagesRDD.flatMap { page =>
      takeURL(pageUrl(page), conf.commonConf) match {
        case Success(content) =>
          codePattern.findAllMatchIn(content).map(_.group(1)).toSeq.distinct
        case Failure(_) => Seq.empty[String]
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
        case Success(content) =>
          val endText: String = "</article>"

          val fullText: String = content
          val startIdx: Integer = fullText.indexOf("""<article class="row vacancy">""")
          val endIdx: Integer = fullText.indexOf(endText, startIdx) + endText.length

          Some(code + fullText.substring(startIdx, endIdx).replace("\n", ""))
        case Failure(_) => None
      }
    }
  }

  println(s"Total vacancies loaded: ${vacanciesRDD.count()}")
  println(vacanciesRDD.take(3).mkString("Array(", ", ", ")"))

  private val outputPath = conf.commonConf.fs.getPath(FolderName.Raw)
  ss.sparkContext.hadoopConfiguration.set("fs.defaultFS", conf.commonConf.fs.url)
  private val fs = FileSystem.get(ss.sparkContext.hadoopConfiguration)

  if (fs.exists(new Path(outputPath))) {
    fs.delete(new Path(outputPath), true)
  }

  vacanciesRDD.repartition(conf.rawPartitions).saveAsTextFile(outputPath)

  // Data Frame version (works not well)
  /*import ss.implicits._

  private val pagesPattern: Regex = """<small>страниц (\d+)</small>""".r
  private val codePattern: Regex = """/vacancy/([a-z0-9]{24})"""".r

  private val extractCodesUDF = udf { (content: String) =>
    if (content != null && content.nonEmpty) {
      codePattern.findAllMatchIn(content).map(_.group(1)).toSeq.distinct
    } else {
      Seq.empty[String]
    }
  }

  private val extractVacancyTextUDF = udf { (content: String) =>
    if (content != null && content.nonEmpty) {
      val startIdx = content.indexOf("""<main id="body" class="container">""")
      if (startIdx >= 0) {
        val endIdx = content.indexOf("</main>", startIdx)
        if (endIdx > startIdx) Some(content.substring(startIdx, endIdx))
        else None
      } else None
    } else None
  }

  private def getVacancyCodesDF: DataFrame = {
    val firstPageContent = takeURL(pageUrl(1), conf).get
    val totalPages = pagesPattern.findFirstMatchIn(firstPageContent).get.group(1).toInt
    val pagesToProcess = math.min(totalPages, conf.pagelimit())

    val pagesDF = (2 to pagesToProcess).toDF("page_number")
      .repartition(conf.partitions())

    val getPageContentUDF = udf { (page: Int) =>
      takeURL(pageUrl(page), conf) match {
        case Success(content) => content
        case Failure(_) => ""
      }
    }

    val otherPagesCodesDF = pagesDF
      .withColumn("content", getPageContentUDF($"page_number"))
      .withColumn("codes", explode(extractCodesUDF($"content")))
      .select("codes")
      .distinct()

    val firstPageCodes = codePattern.findAllMatchIn(firstPageContent)
      .map(_.group(1))
      .toSeq.distinct
      .toDF("codes")

    firstPageCodes.union(otherPagesCodesDF).distinct()
  }

  private def getVacanciesDF(codesDF: DataFrame): DataFrame = {
    val getVacancyContentUDF = udf { (code: String) =>
      takeURL(vacancyUrl(code), conf) match {
        case Success(content) => content
        case Failure(_) => ""
      }
    }

    codesDF
      .withColumn("content", getVacancyContentUDF(col("codes")))
      .withColumn("vacancy_text", extractVacancyTextUDF(col("content")))
      .filter(col("vacancy_text").isNotNull)
      .select("codes", "vacancy_text")
  }

  private val codesDF = getVacancyCodesDF
  println(s"Total codes: ${codesDF.count()}")

  private val vacanciesDF = getVacanciesDF(codesDF)
  println(s"Total vacancies loaded: ${vacanciesDF.count()}")

  give(
    conf = conf,
    folderName = FolderName.Raw,
    data = vacanciesDF.repartition(conf.partitions())
  )
  */


  stopSpark()

  private def pageUrl(page: Integer): String = {
    s"https://geekjob.ru/vacancies/$page"
  }

  private def vacancyUrl(code: String): String = {
    s"https://geekjob.ru/vacancy/$code"
  }
}
