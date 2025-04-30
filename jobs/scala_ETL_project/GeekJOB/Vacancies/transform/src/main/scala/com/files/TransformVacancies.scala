package com.files

import EL.Load.give
import Spark.SparkApp
import com.Config.FolderName.FolderName
import com.Config.{FolderName, LocalConfig}
import com.LoadDB.LoadDB
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.select.Elements
import org.rogach.scallop.ScallopOption

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.sql.Timestamp
import scala.jdk.CollectionConverters.asScalaBufferConverter
import scala.util.matching.Regex

object TransformVacancies extends App with SparkApp {

  private val conf = new LocalConfig(args, "gj") {
    val partitions: ScallopOption[Int] = opt[Int](default = Some(2), validate = _ > 0)

    define()
  }

  override val ss: SparkSession = defineSession(conf.fileConf)
  private val sc: SparkContext = ss.sparkContext

  private val currency: DataFrame = LoadDB.take(ss, conf.fileConf, FolderName.Currency).select(col("id").as("c_id"), col("code").as("c_code"))
  private val currencyCodes: Array[String] = currency.select("c_code").collect().map(_.getString(0))

  private val vacanciesRaw: RDD[String] = sc.textFile(conf.fileConf.fs.getPath(FolderName.Raw), conf.partitions())

  private val schema = StructType(Seq(
    StructField("id", StringType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("employer", StringType, nullable = true),
    StructField("experience", StringType, nullable = true),
    StructField("locations", ArrayType(StringType), nullable = true),
    StructField("publish_date", TimestampType, nullable = true),
    StructField("job_format", ArrayType(StringType), nullable = true),
    StructField("salary_from", LongType, nullable = true),
    StructField("salary_to", LongType, nullable = true),
    StructField("currency", StringType, nullable = true),
    StructField("specs", ArrayType(StringType), nullable = true),
    StructField("fields", ArrayType(StringType), nullable = true),
    StructField("level", ArrayType(StringType), nullable = true)
  ))

  private val transformVacRDD: RDD[Row] = vacanciesRaw.flatMap (str => {

    val id: String = str.substring(0, 24)

    val doc: Document = Jsoup.parse(str.substring(24))

    // headers
    val headers: Elements = doc.select("header")


    val name: String = headers.select("h1").text()

    val comp = headers.select("h5.company-name")

    val company = if (comp.html().contains("Частный рекрутер")) { null } else {
      val a = comp.select("a").first()

      if (a == null) {
        null
      } else {
        a.text()
      }
    }


    val locations = headers.select("div.location").text()
      .split(", ").map(_.trim)

    val date: Timestamp = dateTransform(headers.select("div.time").text())


    val jobInfo: Elements = headers.select("div.jobinfo")

    val (exp, jobFormat) = transformJobFormat(jobInfo
      .select("span.jobformat").html())


    val (sals, currency) = salaryTransform(jobInfo.select("span.salary").text())
    val salary_from = if (sals.nonEmpty) { sals.head } else { null }
    val salary_to = if (sals.length > 1) { sals(1) } else { null }


    // tags
    val (specs, fields, level) = tagsTransform(doc.select("div.tags-list"))


    Some(Row(
      id,
      name,
      company,
      exp,
      locations,
      date,
      jobFormat,
      salary_from,
      salary_to,
      currency,
      specs,
      fields,
      level
    ))
  })


  private val genVacHelper: DataFrame = ss.createDataFrame(transformVacRDD, schema)

  private val genVac: DataFrame = genVacHelper
    .join(currency, genVacHelper("currency") === currency("c_code"), "left_outer")
    .withColumn("currency_id",
      when(col("c_code").isNotNull, col("c_id"))
        .otherwise(col("currency")))


  private val transformVac: DataFrame = genVac.select(
    "id", "name", "employer", "experience", "publish_date", "salary_from", "salary_to", "currency_id"
  )

  private val locations: DataFrame = genVac
    .select(col("id"), explode(col("locations")).as("name"))
    .dropDuplicates(Seq("id", "name"))

  private val jobFormat: DataFrame = genVac
    .select(col("id"), explode(col("job_format")).as("name"))
    .dropDuplicates(Seq("id", "name"))

  private val specs: DataFrame = genVac
    .select(col("id"), explode(col("specs")).as("name"))
    .dropDuplicates(Seq("id", "name"))

  private val fields: DataFrame = genVac
    .select(col("id"), explode(col("fields")).as("name"))
    .dropDuplicates(Seq("id", "name"))

  private val levels: DataFrame = genVac
    .select(col("id"), explode(col("level")).as("name"))
    .dropDuplicates(Seq("id", "name"))


  save(FolderName.Vac, transformVac, conf.partitions())
  save(FolderName.Locations, locations)
  save(FolderName.JobFormats, jobFormat)
  save(FolderName.Skills, specs)
  save(FolderName.Fields, fields)
  save(FolderName.Levels, levels)

  stopSpark()


  private def dateTransform(raw: String): Timestamp = {
    val monthsMap: Map[String, String] = Map(
      "января" -> "01",
      "февраля" -> "02",
      "марта" -> "03",
      "апреля" -> "04",
      "мая" -> "05",
      "июня" -> "06",
      "июля" -> "07",
      "августа" -> "08",
      "сентября" -> "09",
      "октября" -> "10",
      "ноября" -> "11",
      "декабря" -> "12"
    )

    val times: Array[String] = raw.split(" ")
    val day: String = {
      if (times(0).length < 2) {
        s"0${times(0)}"
      } else {
        times(0)
      }

    }
    val month: String = monthsMap(times(1).toLowerCase)
    val year: String = if (times.length > 2) {
      times(2)
    } else {
      conf.date().substring(0, 4)
    }

    val dateStr: String = s"$year.$month.$day"

    try {
      val formatter = DateTimeFormatter.ofPattern("yyyy.MM.dd")
      val localDate = java.time.LocalDate.parse(dateStr, formatter)
      Timestamp.valueOf(localDate.atStartOfDay())
    } catch {
      case _: Exception => null
    }
  }

  private def transformJobFormat(raw: String): (String, Array[String]) = {
    val temp = raw.split("<br>").map(_.trim)

    val expOption: Option[String] = temp.find(_.toLowerCase.contains("опыт"))

    val exp = expOption match {
      case Some(t) => {
        val numbers = """\d+""".r.findAllIn(t).map(_.toInt).toList
        if (numbers.size == 2) {
          val f = if (numbers.head == 1) "года " else ""
          val s = if (numbers(1) > 1) "лет" else "года"
          s"От ${numbers.head} ${f}до ${numbers(1)} $s"
        } else if (numbers.size == 1) {
          val f = if (numbers.head == 1) "года" else "лет"
          val op = if (t.contains("менее")) "Менее" else "Более"
          s"$op ${numbers.head} $f"
        } else {
          "Любой"
        }
      }
      case None => null
    }

    (exp, temp.filter(o => !o.toLowerCase.contains("опыт")))
  }

  private def salaryTransform(raw: String): (Array[Long], String) = {
    val numberPattern: Regex = """(\d{1,3}(?:\s\d{3})*)""".r
    val currencyPattern: Regex = s"""([${currencyCodes.filter(_.length == 1).mkString("", "", "")}])""".r

    val salaries = numberPattern.findAllIn(raw)
      .map(_.replaceAll("\\s", "").toLong).toArray

    val currency = currencyPattern.findFirstIn(raw).orNull

    (salaries, currency)
  }

  private def tagsTransform(divTag: Elements): (Array[String], Array[String], Array[String]) = {
    (
      divTag.select("b:contains(Специализация) + br ~ a.chip:not(b:contains(Отрасль) ~ a.chip)").eachText().asScala.toArray,
      divTag.select("b:contains(Отрасль и сфера применения) + br ~ a.chip:not(b:contains(Уровень) ~ a.chip)").eachText().asScala.toArray,
      divTag.select("b:contains(Уровень должности) + br ~ a.chip").eachText().asScala.toArray
    )
  }

  private def save(folderName: FolderName, dataFrame: DataFrame, repartition: Integer = 1): Unit = {
    give(
      conf = conf.fileConf,
      data = dataFrame.repartition(repartition),
      folderName = folderName
    )
  }
}