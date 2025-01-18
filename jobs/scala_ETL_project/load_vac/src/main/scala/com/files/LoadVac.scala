package com.files

import EL.Extract.take
import com.LoadDB.LoadDB.give
import Spark.SparkApp
import org.apache.spark.sql.SaveMode

object LoadVac extends App with SparkApp {

  give(take(isRoot = false, "transformed").get, "vacancies", saveMode = SaveMode.Append)

  stopSpark()
}
