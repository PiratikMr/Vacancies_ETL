package com.files

import EL.Extract.take
import Spark.SparkApp
import com.LoadDB.LoadDB.give

object LoadDict extends App with SparkApp {
  load("areas")
  load("currency")
  load("schedule")
  load("employment")
  load("experience")

  stopSpark()

  private def load(name: String): Unit = give(take(isRoot = true, name).get, name)
}
