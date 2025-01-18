package com.files

import EL.Load.give
import Spark.SparkApp
import com.extractURL.ExtractURL.takeURL
import org.apache.spark.sql.functions.{col, count, explode, when}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, functions}

import scala.annotation.tailrec

object ExtractDict extends App with SparkApp {

  // areas //
  val dfA: DataFrame = toDF(takeURL("https://api.hh.ru/areas").get)
  val transformedA: DataFrame = {
    @tailrec
    def f(acc: DataFrame = dfA.drop("areas"), i: DataFrame = dfA): DataFrame = {
      if (i.agg(count(when(functions.size(col("areas")).gt(0), 1))).first().getLong(0) == 0) {
        acc.
          withColumn("id", col("id").cast(LongType)).
          withColumn("parent_id", col("parent_id").cast(LongType))
      }
      else {
        val next: DataFrame = i.withColumn("areas", explode(col("areas"))).select("areas.*")
        f(acc.union(next.drop("areas")),next)
      }
    }
    f()
  }
  give(isRoot = true, transformedA, "areas")

  // roles //
  val dfR: DataFrame = toDF(takeURL("https://api.hh.ru/professional_roles").get)
  val transformedR: DataFrame = dfR
      .withColumn("categories", explode(col("categories")))
      .select("categories.*")
      .withColumn("id", col("id").cast(LongType))
      .withColumn("roles", explode(col("roles")))
      .select(col("id").as("parent_id"),
        col("roles").getField("id").cast(LongType).as("id"),
        col("roles").getField("name").as("name"))
  give(isRoot = true, transformedR, "roles")

  // dictionaries //
  val dfD: DataFrame = toDF(takeURL("https://api.hh.ru/dictionaries").get)
  give(isRoot = true, expl(dfD,"currency")
    .withColumn("id", col("code"))
    .select("id", "name", "rate"), "currency")

  give(isRoot = true, expl(dfD,"schedule")
    .select("id", "name"), "schedule")

  give(isRoot = true, expl(dfD,"employment")
    .select("id", "name"), "employment")

  give(isRoot = true, expl(dfD,"experience")
    .select("id", "name"), "experience")


  stopSpark()



  private def expl(df: DataFrame, field: String): DataFrame = df
    .withColumn(s"$field", explode(col(s"$field"))).select(s"$field.*")
  private def toDF(s: String): DataFrame = {
    import ss.implicits._
    ss.read.json(Seq(s).toDS())
  }
}
