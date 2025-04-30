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

  private val conf = new LocalConfig(args, "hh") {
    val prId: ScallopOption[Int] = opt[Int](name = "fid", default = Some(11), validate = _ > 0)

    val perPage: ScallopOption[Int] = opt[Int](name = "ppage", default = Some(100), validate = _ > 0)
    val pages: ScallopOption[Int] = opt[Int](default = Some(20), validate = _ > 0)
    val urlsPerSecond: ScallopOption[Int] = opt[Int](name = "urlsps", default = Some(20), validate = _ > 0)

    val partitions: ScallopOption[Int] = opt[Int](default = Some(6), validate = _ > 0)

    define()
  }

  override val ss: SparkSession = defineSession(conf.fileConf)

  import ss.implicits._


  private val ids: Seq[Long] = take(
    ss = ss,
    conf = conf.fileConf,
    folderName = FolderName.Dict(FolderName.Roles)
  ).get
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
      .json(Seq(takeURL(url(id), conf.fileConf).get).toDS())
    val found: Long = df.first().getAs[Long]("found")

    f(Math.min(found / conf.perPage(), conf.pages() - 1))
  })


  println(s"URLs to read: ${urlList.length}")
  private var readURLs: Integer = 1

  private val initData: Dataset[String] = Seq(takeURL(urlList.head, conf.fileConf).get).toDS()
  private val data: Dataset[String] = urlList.tail.foldLeft(initData)((df, url) => {
    Thread.sleep(1000 / conf.urlsPerSecond())
    val data:String = takeURL(url, conf.fileConf) match {
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


  give(
    conf = conf.fileConf,
    folderName = FolderName.Raw,
    data = df
  )

  stopSpark()


  private def url(id: Long, perPage: Int = 0, page: Long = 0): String =
    s"https://api.hh.ru/vacancies?page=$page&per_page=$perPage&professional_role=$id"
}