package com.files

import com.files.URLHandler.readURL
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ExtractVacancies extends SparkApp {

  private class Conf(args: Array[String]) extends LocalConfig(args) {
    lazy val fieldId: Int = getFromConfFile[Int]("fieldId")
    lazy val vacsPerPage: Int = getFromConfFile[Int]("vacsPerPage")
    lazy val pageLimit: Int = getFromConfFile[Int]("pageLimit")
    lazy val urlsPerSecond: Int = getFromConfFile[Int]("urlsPerSecond")
    lazy val rawPartitions: Int = getFromConfFile[Int]("rawPartitions")

    define()
  }


  def main(args: Array[String]): Unit = {

    val conf: Conf = new Conf(args)
    val spark: SparkSession = defineSession(conf.commonConf)

    val ids: Seq[Long] = getProfessionalIds(spark, conf)

    val df: DataFrame = getDataFromUrls(spark, conf, ids)
    println(s"\n\n\nTotal vacancies: ${df.count()}\n\n\n")

    HDFSHandler.save(conf.commonConf)(FolderName.Raw, df)

    spark.stop()

  }


  private def getProfessionalIds(spark: SparkSession, conf: Conf): Seq[Long] = {
    HDFSHandler.load(spark, conf.commonConf)(FolderName.Roles)
      .where(col("parent_id").equalTo(conf.fieldId))
      .select("id")
      .collect()
      .map(r => r.getLong(0))
      .toSeq
  }


  private def getDataFromUrls(spark: SparkSession, conf: Conf, ids: Seq[Long]): DataFrame = {
    import spark.implicits._

    val urlSeq: Seq[String] =
      ids.map (id => {
          val res: Long = readURL(url(id), conf.commonConf) match {
            case Some(body) => spark.read.json(Seq(body).toDS()).first().getAs[Long]("found")
            case _ => 0L
          }
          (id, math.min(res / conf.vacsPerPage, conf.pageLimit - 1))
        })
        .filter { case(_, found) => found <= 0 }
        .map { case(id, found) => url(id, conf.vacsPerPage, found) }

    println(s"\n\n\nURLs to read: ${urlSeq.length}\n\n\n")

    // dataset of body urls
    val data: Dataset[String] = urlSeq.foldLeft(Seq.empty[String])((acc, url) => {
      Thread.sleep(1000 / conf.urlsPerSecond)
      readURL(url, conf.commonConf) match {
        case Some(body) => body +: acc
        case _ => acc
      }
    }).toDS

    println(s"\n\n\nRead URLs: ${data.count()}\n\n\n")

    // final DataFrame of body urls
    spark.read.json(data.repartition(conf.rawPartitions))
      .withColumn("items", explode(col("items")))
      .select("items.*")
  }

  private def url(id: Long, perPage: Int = 0, page: Long = 0): String =
    s"https://api.hh.ru/vacancies?page=$page&per_page=$perPage&professional_role=$id"
}