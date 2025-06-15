package com.files

import EL.Extract.take
import EL.Load.give
import Spark.SparkApp
import com.Config.{FolderName, LocalConfig}
import com.extractURL.ExtractURL.takeURL
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.rogach.scallop.ScallopOption

import scala.annotation.tailrec
import scala.util.{Failure, Success}

object ExtractVacancies extends App with SparkApp {

  private val conf = new LocalConfig(args) {
    lazy val fieldId: Int = getFromConfFile[Int]("fieldId")
    lazy val vacsPerPage: Int = getFromConfFile[Int]("vacsPerPage")
    lazy val pageLimit: Int = getFromConfFile[Int]("pageLimit")
    lazy val urlsPerSecond: Int = getFromConfFile[Int]("urlsPerSecond")
    lazy val rawPartitions: Int = getFromConfFile[Int]("rawPartitions")

    define()
  }

  override val ss: SparkSession = defineSession(conf.commonConf)

  import ss.implicits._


  private val ids: Seq[Long] = take(
    ss = ss,
    conf = conf.commonConf,
    folderName = FolderName.Dict(FolderName.Roles)
  ).get
    .where(col("parent_id").equalTo(conf.fieldId))
    .select("id")
    .collect()
    .map(r => r.getLong(0))
    .toSeq

  private val urlList: Seq[String] = ids.flatMap(id => {
    @tailrec
    def f(i: Long, acc: List[String] = List[String]()): List[String] = {
      if (i == -1) acc
      else f(i - 1, acc :+ url(id, conf.vacsPerPage, i))
    }

    val found: Long = takeURL(url(id), conf.commonConf) match {
      case Some(body) => ss.read.json(Seq(body).toDS()).first().getAs[Long]("found")
      case _ => 0L
    }

    f(Math.min(found / conf.vacsPerPage, conf.pageLimit - 1))
  })


  println(s"URLs to read: ${urlList.length}")
  private var readURLs: Integer = 1

  private val initData: Dataset[String] = Seq(takeURL(urlList.head, conf.commonConf).get).toDS()
  private val data: Dataset[String] = urlList.tail.foldLeft(initData)((df, url) => {
    Thread.sleep(1000 / conf.urlsPerSecond)
    val data:String = takeURL(url, conf.commonConf) match {
      case Some(body) =>
        readURLs = readURLs + 1
        body
      case _ => ""
    }
    df.union(Seq(data).toDS())
  })

  println(s"Read URLs: $readURLs")


  private val df: DataFrame = ss.read.json(data.repartition(conf.rawPartitions))
    .withColumn("items", explode(col("items")))
    .select("items.*")

  println(s"Vacancies total count: ${df.count()}")


  give(
    conf = conf.commonConf,
    folderName = FolderName.Raw,
    data = df
  )

  stopSpark()


  private def url(id: Long, perPage: Int = 0, page: Long = 0): String =
    s"https://api.hh.ru/vacancies?page=$page&per_page=$perPage&professional_role=$id"
}