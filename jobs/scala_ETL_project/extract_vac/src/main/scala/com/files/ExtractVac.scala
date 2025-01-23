package com.files

import EL.Load.give
import com.extractURL.ExtractURL.takeURL
import EL.Extract.take
import Spark.SparkApp
import com.Config.HDFSConfig
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.annotation.tailrec
import scala.util.{Failure, Success}

object ExtractVac extends App with SparkApp {

  private val conf = new ScallopConf(args) {
    val prId: ScallopOption[Int] = opt[Int](name = "fid", default = Some(11), validate = _ > 0)

    val perPage: ScallopOption[Int] = opt[Int](name = "ppage", default = Some(100), validate = _ > 0)
    val pages: ScallopOption[Int] = opt[Int](default = Some(20), validate = _ > 0)
    val urlsPerSecond: ScallopOption[Int] = opt[Int](name = "urlsps", default = Some(20), validate = _ > 0)

    val outFileName: ScallopOption[String] = opt[String](default = Some(HDFSConfig.vacanciesRawFileName))
    val partitions: ScallopOption[Int] = opt[Int](default = Some(6), validate = _ > 0)


    verify()
  }

  import ss.implicits._


  private val ids: Seq[Long] = take(isRoot = true, fileName = "roles").get
    .where(col("parent_id").equalTo(conf.prId()))
    .select("id")
    .collect()
    .map(r => r.getLong(0))
    .toSeq

  private val urlList: Seq[String] = ids.flatMap(id => {
    @tailrec
    def f(i: Long, acc: List[String] = List[String]()): List[String] = {
      if (i == -1) acc
      else f(i - 1, acc :+ url(id, conf.perPage(), i))
    }

    val df: DataFrame = ss.read
      .json(Seq(takeURL(url(id)).get).toDS())
    val found: Long = df.first().getAs[Long]("found")

    f(Math.min(found / conf.perPage(), conf.pages() - 1))
  })


  println(s"URLs to read: ${urlList.length}")
  private var readURLs: Integer = 1

  private val initData: Dataset[String] = Seq(takeURL(urlList.head).get).toDS()
  private val data: Dataset[String] = urlList.tail.foldLeft(initData)((df, url) => {
    Thread.sleep(1000 / conf.urlsPerSecond())
    val data:String = takeURL(url) match {
      case Success(v) =>
        readURLs = readURLs + 1
        v
      case Failure(e) =>
        println(e)
        ""
    }
    df.union(Seq(data).toDS())
  })

  println(s"Read URLs: $readURLs")


  private val df: DataFrame = ss.read.json(data.repartition(conf.partitions()))
    .withColumn("items", explode(col("items")))
    .select("items.*")

  println(s"Vacancies total count: ${df.count()}")


  give(isRoot = false, data = df, fileName = conf.outFileName())

  stopSpark()


  private def url(id: Long, perPage: Int = 0, page: Long = 0): String =
    s"https://api.hh.ru/vacancies?page=$page&per_page=$perPage&professional_role=$id"
}

