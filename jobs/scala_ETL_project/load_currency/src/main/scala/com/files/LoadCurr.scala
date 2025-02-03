package com.files

import EL.Extract.take
import Spark.SparkApp
import com.LoadDB.LoadDB.give
import org.apache.spark.sql.SaveMode

object LoadCurr extends App with SparkApp {
  give(take(isRoot = true, fileName = "currency").get, "currency", saveMode = SaveMode.Append)
  stopSpark()
}
