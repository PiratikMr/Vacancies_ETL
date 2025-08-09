package com.files

import URLHandler._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object ExtractVacancies extends App with SparkApp {

  private class Conf(args: Array[String]) extends LocalConfig(args) {
    lazy val vacsLimit: Int = math.max(1, getFromConfFile[Int]("vacsLimit"))
    lazy val rawPartitions: Int = getFromConfFile[Int]("rawPartitions")
    lazy val inDays: String = getFromConfFile[String]("inDays")

    define()
  }
  private val conf: Conf = new Conf(args)
  private val spark: SparkSession = defineSession(conf.sparkConf)

  private def URL(limit: Long): String = s"https://getmatch.ru/api/offers?pa=${conf.inDays}d&limit=$limit"


  import spark.implicits._


  private val totalVacs: Long = math.min({
    val body: String = readOrDefault(URL(1), conf.urlConf, """{"meta":{"total":0}}""")
    """"total"\s*:\s*(\d+)""".r.findFirstMatchIn(body).get.group(1).toLong
  }, conf.vacsLimit)


  private val mainBody: String = readOrDefault(URL(totalVacs), conf.urlConf)
  private val data: Dataset[String] = Seq(mainBody).toDS.repartition(conf.rawPartitions)
  data.write.mode(SaveMode.Overwrite).text(conf.fsConf.getPath(FolderName.RawVacancies))

  spark.stop()
}
