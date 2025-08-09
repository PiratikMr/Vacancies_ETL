package com.files

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.jsoup.select.Elements

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters.asScalaBufferConverter
import scala.util.matching.Regex

object TransformVacancies extends App with SparkApp {

  implicit class DataFrameOps(df: DataFrame) {
    def save(field: String, fn: FolderName): Unit = {
      df.select(col("id"), explode(col(field)).as("name")).dropDuplicates(Seq("id", "name"))
        .save(fn)
    }

    def save(fn: FolderName): Unit = HDFSHandler.saveParquet(df, conf.fsConf.getPath(fn))
  }

  private class Conf(args: Array[String]) extends LocalConfig(args) {
    lazy val transformPartitions: Int = getFromConfFile[Int]("transformPartitions")
    define()
  }

  private val conf: Conf = new Conf(args)
  private val spark: SparkSession = defineSession(conf.sparkConf)

  private val currencyDF: DataFrame = DBHandler.load(spark, conf.dbConf, FolderName.Currency)
    .select(col("id").as("c_id"), col("code").as("c_code"))

  private val rawDataDS: Dataset[String] = spark.read.textFile(conf.fsConf.getPath(FolderName.RawVacancies))

  private val schema: StructType = StructType(Seq(
        StructField("id", StringType, nullable = false),
        StructField("title", StringType, nullable = true),
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


  private val currencyPattern: Regex = {
    val tmp: String = currencyDF.select("c_code").collect().map(_.getString(0)).filter(_.length == 1).mkString("", "", "")
    s"""([$tmp])""".r
  }
  private val currYear: String = conf.currDate().substring(0, 4)


  private val transformedDF: DataFrame = {
    val rowsRDD = rawDataDS.rdd.mapPartitions { iterator => iterator.map { row => transformHelper(row) }
    }

    spark.createDataFrame(rowsRDD, schema)
      .join(currencyDF, col("currency") === currencyDF("c_code"), "left_outer")
      .withColumn("salary_currency_id",
        when(col("c_code").isNotNull, col("c_id"))
          .otherwise(col("currency")))
      .withColumn("url", concat(lit("https://geekjob.ru/vacancy/"), col("id")))
      .withColumn("published_at", col("publish_date"))
      .withColumn("is_active", lit(true))
  }.repartition(conf.transformPartitions)


  transformedDF.select("id", "title", "employer", "experience", "salary_from", "salary_to", "salary_currency_id",
    "url", "published_at", "is_active").save(FolderName.Vacancies)

  transformedDF.save("locations", FolderName.Locations)
  transformedDF.save("job_format", FolderName.JobFormats)
  transformedDF.save("specs", FolderName.Skills)
  transformedDF.save("fields", FolderName.Fields)
  transformedDF.save("level", FolderName.Levels)

  spark.stop()



  private def transformHelper(row: String): Row = {
    val Doc: Document = Jsoup.parse(row.substring(24))
    val Header: Elements = Doc.select("header")
    val Company: Elements = Header.select("h5.company-name")
    val JobInfo: Elements = Header.select("div.jobinfo")
    val Salary: String = JobInfo.select("span.salary").text()
    val SalaryArray: Array[Long] = """(\d{1,3}(?:\s\d{3})*)""".r.findAllIn(Salary).map(_.replaceAll("\\s", "").toLong)
      .toArray


    val id: String = row.substring(0, 24)
    val title: String = Header.select("h1").text()

    val company = if (Company.html().contains("Частный рекрутер")) null
      else Company.select("a").first() match { case a: Element => a.text() case _ => null }

    val locations = Header.select("div.location").text().split(", ").map(_.trim)

    val date: Timestamp = dateTransform(Header.select("div.time").text())

    val (exp, jobFormat) = transformJobFormat(JobInfo.select("span.jobformat").html())

    val salary_from = if (SalaryArray.nonEmpty) SalaryArray.head else null
    val salary_to = if (SalaryArray.length > 1) SalaryArray(1) else null

    val currency: String = currencyPattern.findFirstIn(Salary).orNull

    val (specs, fields, level) = tagsTransform(Doc.select("div.tags-list"))


    Row(id, title, company, exp, locations, date, jobFormat, salary_from, salary_to, currency, specs, fields, level)
  }

  private lazy val monthsMap: Map[String, String] = Map(
    "января" -> "01", "февраля" -> "02", "марта" -> "03", "апреля" -> "04",
    "мая" -> "05", "июня" -> "06", "июля" -> "07", "августа" -> "08",
    "сентября" -> "09", "октября" -> "10", "ноября" -> "11", "декабря" -> "12"
  )
  private def dateTransform(raw: String): Timestamp = {
    val times: Array[String] = raw.split(" ")

    val day: String = if (times(0).length < 2) s"0${times(0)}" else times(0)
    val year: String = if (times.length > 2) times(2) else currYear

    val dateStr: String = s"$year.${monthsMap(times(1))}.$day"

    try {
      Timestamp.valueOf(java.time.LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyy.MM.dd")).atStartOfDay())
    } catch { case _: Exception => null }
  }

  private def transformJobFormat(raw: String): (String, Array[String]) = {
    val temp = raw.split("<br>").map(_.trim)

    val expOption: Option[String] = temp.find(_.toLowerCase.contains("опыт"))

    val exp = expOption match {
      case Some(t) =>
        val numbers = """\d+""".r.findAllIn(t).map(_.toInt).toList
        if (numbers.size == 2) {
          val f = if (numbers.head == 1) "года " else ""
          val s = if (numbers(1) > 1) "лет" else "года"
          s"От ${numbers.head} ${f}до ${numbers(1)} $s"
        } else if (numbers.size == 1) {
          val f = if (numbers.head == 1) "года" else "лет"
          val op = if (t.contains("менее")) "Менее" else "Более"
          s"$op ${numbers.head} $f"
        } else "Любой"
      case None => null
    }

    val jobFormat = temp.filter(o => !o.toLowerCase.contains("опыт")).flatMap(_.split("•")).map(_.trim)
      .filter(_.nonEmpty)

    (exp, jobFormat)
  }

  private def tagsTransform(divTag: Elements): (Array[String], Array[String], Array[String]) = {

    val specs_t: Array[String] = divTag.select("b:contains(Специализация) + br ~ a.chip:not(b:contains(Отрасль) ~ a.chip)").eachText().asScala.toArray
    val specs: Array[String] = specs_t.filter(str => "[а-яА-ЯёЁ]".r.findFirstIn(str).isEmpty)

    (
      specs,
      divTag.select("b:contains(Отрасль и сфера применения) + br ~ a.chip:not(b:contains(Уровень) ~ a.chip)").eachText().asScala.toArray,
      divTag.select("b:contains(Уровень должности) + br ~ a.chip").eachText().asScala.toArray
    )
  }
}