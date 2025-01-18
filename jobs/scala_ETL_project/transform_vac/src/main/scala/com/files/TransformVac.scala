package com.files

import EL.Extract.take
import EL.Load.give
import Spark.SparkApp
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Row}

import scala.annotation.tailrec

object TransformVac extends App with SparkApp {

  private val areas: Map[Long, Option[Long]] = take(isRoot = true, "areas").get.rdd
    .map(row => {
      val id: Long = row.getAs[Long]("id")
      val parent_id: Option[Long] = Option(row.getAs[Long]("parent_id"))
      (id, parent_id)
    }).collectAsMap().toMap

  private val getCountry: Long => Long = id => {
    @tailrec
    def f(i: Long = id): Long = {
      areas.apply(i) match {
        case Some(v) => f(v)
        case None => i
      }
    }
    f()
  }

  private val country_id: UserDefinedFunction = udf((row: Row) => getCountry(row.getAs[String]("id").toLong))

  private val df: DataFrame = take(isRoot = false, "vacancies").get
    .withColumn("id", col("id").cast(LongType))

    .withColumn("region_area_id", col("area").getField("id").cast(LongType))
    .withColumn("country_area_id", country_id.apply(col("area")))

    .withColumn("salary_from", col("salary").getField("from"))
    .withColumn("salary_to", col("salary").getField("to"))

    .withColumn("close_to_metro", col("address").getField("metro_stations").isNotNull)

    .withColumn("schedule_id", col("schedule").getField("id"))
    .withColumn("experience_id", col("experience").getField("id"))
    .withColumn("employment_id", col("employment").getField("id"))
    .withColumn("currency_id", col("salary").getField("currency"))

    .select("id", "name", "region_area_id", "country_area_id", "salary_from", "salary_to", "close_to_metro",
      "schedule_id", "experience_id", "employment_id", "currency_id")

    .dropDuplicates("id")

  give(isRoot = false, df.repartition(1), "transformed")
  stopSpark()
}