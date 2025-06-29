package com.files

import com.files.FolderName.FolderName
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.select.Elements

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters.asScalaBufferConverter
import scala.util.matching.Regex

object TransformVacancies extends SparkApp {

  private class Conf(args: Array[String]) extends LocalConfig(args) {
    lazy val transformPartitions: Int = getFromConfFile[Int]("transformPartitions")
    define()
  }


  def main(args: Array[String]): Unit = {

    val conf: Conf = new Conf(args)
    val spark: SparkSession = defineSession(conf.commonConf)


    val currency: DataFrame = DBHandler.load(spark, conf.commonConf, conf.tableName(FolderName.Currency))
      .select(col("id").as("c_id"), col("code").as("c_code"))

    val rawData: RDD[String] = spark.sparkContext.textFile(conf.commonConf.fs.getPath(FolderName.Raw), conf.transformPartitions)


    val vacancies: DataFrame = transformData(spark, conf, rawData, currency)

    saveData(conf, vacancies)

    spark.stop()

  }



  private def transformData(spark: SparkSession, conf: Conf, rawData: RDD[String], currency: DataFrame): DataFrame = {

    val year: String = conf.fileName().substring(0, 4)
    val currencyPattern: String = currency.select("c_code")
      .collect().map(_.getString(0))
      .filter(_.length == 1).mkString("", "", "")

    val schema = StructType(Seq(
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
    val transformRDD: RDD[Row] = rawData.flatMap(row => transformHelper(row, currencyPattern, year))

    val df: DataFrame = spark.createDataFrame(transformRDD, schema)

    df.join(currency, df("currency") === currency("c_code"), "left_outer")
      .withColumn("currency_id",
        when(col("c_code").isNotNull, col("c_id"))
          .otherwise(col("currency")))
  }

  private def transformHelper(row: String, currencyPattern: String, year: String): Option[Row] = {
    //
    val id: String = row.substring(0, 24)
    //

    val doc: Document = Jsoup.parse(row.substring(24))

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

    val date: Timestamp = dateTransform(headers.select("div.time").text(), year)


    val jobInfo: Elements = headers.select("div.jobinfo")

    val (exp, jobFormat) = transformJobFormat(jobInfo
      .select("span.jobformat").html())


    val (sals, currency) = salaryTransform(currencyPattern, jobInfo.select("span.salary").text())
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
  }



  private def saveData(conf: Conf, df: DataFrame): Unit = {

    val save = saveHelper(conf.commonConf)_

    val transformVac: DataFrame = df.select(
      "id", "name", "employer", "experience", "publish_date", "salary_from", "salary_to", "currency_id"
    )
    save(FolderName.Vac, transformVac)


    val locations: DataFrame = df
      .select(col("id"), explode(col("locations")).as("name"))
      .dropDuplicates(Seq("id", "name"))
    save(FolderName.Locations, locations)


    val jobFormat: DataFrame = df
      .select(col("id"), explode(col("job_format")).as("name"))
      .dropDuplicates(Seq("id", "name"))
    save(FolderName.JobFormats, jobFormat)


    val specs: DataFrame = df
      .select(col("id"), explode(col("specs")).as("name"))
      .dropDuplicates(Seq("id", "name"))
    save(FolderName.Skills, specs)


    val fields: DataFrame = df
      .select(col("id"), explode(col("fields")).as("name"))
      .dropDuplicates(Seq("id", "name"))
    save(FolderName.Fields, fields)


    val levels: DataFrame = df
      .select(col("id"), explode(col("level")).as("name"))
      .dropDuplicates(Seq("id", "name"))
    save(FolderName.Levels, levels)
  }

  private def saveHelper(conf: CommonConfig)
                        (folderName: FolderName, data: DataFrame): Unit = {
    HDFSHandler.save(conf)(folderName, data)
  }




  private def dateTransform(raw: String, fileYear: String): Timestamp = {
    val monthsMap: Map[String, String] = Map(
      "января" -> "01", "февраля" -> "02", "марта" -> "03", "апреля" -> "04",
      "мая" -> "05", "июня" -> "06", "июля" -> "07", "августа" -> "08",
      "сентября" -> "09", "октября" -> "10", "ноября" -> "11", "декабря" -> "12"
    )

    val times: Array[String] = raw.split(" ")


    val day: String = if (times(0).length < 2) s"0${times(0)}" else times(0)

    val month: String = monthsMap(times(1).toLowerCase)

    val year: String = if (times.length > 2) times(2) else fileYear


    val dateStr: String = s"$year.$month.$day"

    try {
      Timestamp.valueOf(java.time.LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyy.MM.dd")).atStartOfDay())
    } catch {
      case _: Exception => null
    }
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
        } else {
          "Любой"
        }
      case None => null
    }

    val jobFormat = temp
      .filter(o => !o.toLowerCase.contains("опыт"))
      .flatMap(_.split("•"))
      .map(_.trim)
      .filter(_.nonEmpty)

    (exp, jobFormat)
  }

  private def salaryTransform(pattern: String, raw: String): (Array[Long], String) = {
    val numberPattern: Regex = """(\d{1,3}(?:\s\d{3})*)""".r
    val currencyPattern: Regex = s"""([$pattern])""".r

    val salaries = numberPattern.findAllIn(raw)
      .map(_.replaceAll("\\s", "").toLong).toArray

    val currency = currencyPattern.findFirstIn(raw).orNull

    (salaries, currency)
  }

  private def tagsTransform(divTag: Elements): (Array[String], Array[String], Array[String]) = {

    val specs_t: Array[String] = divTag.select("b:contains(Специализация) + br ~ a.chip:not(b:contains(Отрасль) ~ a.chip)").eachText().asScala.toArray
    val russianCharsRegex = "[а-яА-ЯёЁ]".r
    val specs: Array[String] = specs_t.filter(str => russianCharsRegex.findFirstIn(str).isEmpty)

    (
      specs,
      divTag.select("b:contains(Отрасль и сфера применения) + br ~ a.chip:not(b:contains(Уровень) ~ a.chip)").eachText().asScala.toArray,
      divTag.select("b:contains(Уровень должности) + br ~ a.chip").eachText().asScala.toArray
    )
  }

}