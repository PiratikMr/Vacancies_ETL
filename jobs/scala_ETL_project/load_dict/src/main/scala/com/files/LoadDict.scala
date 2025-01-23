package com.files

import EL.Extract.take
import Spark.SparkApp
import com.LoadDB.LoadDB.give
import org.apache.spark.sql.DataFrame

object LoadDict extends App with SparkApp {
  load("areas")
  load("currency")
  load("schedule")
  load("employment")
  load("experience")

  private val rolesDF: DataFrame = take(isRoot = true, fileName = "roles").get
    .select("id", "name")
    .dropDuplicates("id")
  give(rolesDF, "roles")

  stopSpark()

  private def load(name: String): Unit = give(take(isRoot = true, fileName = name).get, name)
}
