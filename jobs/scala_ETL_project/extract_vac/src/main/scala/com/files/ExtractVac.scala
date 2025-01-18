package com.files

import EL.Load.give
import com.extractURL.ExtractURL.takeURL
import EL.Extract.take
import Spark.SparkApp
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.annotation.tailrec
import scala.util.{Failure, Success}

object ExtractVac extends App with SparkApp {
  import ss.implicits._

  var sucCount: Integer = 0

  val ids: Seq[Long] = take(isRoot = true, "roles").get
    .where(col("parent_id").equalTo(11))
    .select("id").rdd.map(r => r.getAs[Long](0)).collect().toSeq
  val urlList: Seq[String] = ids.flatMap(id => {
    @tailrec
    def f(i: Long, acc: List[String] = List[String]()): List[String] = {
      if (i == -1) acc
      else f(i - 1, acc :+ url(id, 100, i))
    }

    val df: DataFrame = ss.read
      .json(Seq(takeURL(url(id)).get).toDS())
    val found: Long = df.first().getAs[Long]("found")

    f(Math.min(found / 100, 19))
  })

  println(s"URL count: ${urlList.length}")

  val initData: Dataset[String] = Seq(takeURL(urlList.head).get).toDS()
  val data: Dataset[String] = urlList.tail.foldLeft(initData)((df, url) => {
    Thread.sleep(40)
    val data:String = takeURL(url) match {
      case Success(v) =>
        sucCount = sucCount + 1
        v
      case Failure(e) =>
        println(e)
        ""
    }
    df.union(Seq(data).toDS())
  })

  println(s"Succesful links: $sucCount")

  val df: DataFrame = ss.read.json(data.repartition(6))
    .withColumn("items", explode(col("items")))
    .select("items.*")

  give(isRoot = false, df, "vacancies")

  stopSpark()

  private def url(id: Long, perPage: Long = 0, page: Long = 0): String =
    s"https://api.hh.ru/vacancies?page=$page&per_page=$perPage&professional_role=$id"
}

