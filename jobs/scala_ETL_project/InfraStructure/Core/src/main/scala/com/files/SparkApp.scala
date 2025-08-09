package com.files

import com.files.Common.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkApp extends Serializable {

  def defineSession(conf: SparkConf, parallelism: Int = -1): SparkSession = {
    SparkSession.builder()
      .appName(conf.name)
      .master(conf.master)
      .config("spark.default.parallelism", if (parallelism < 1) conf.defaultParallelism.toString else parallelism.toString)
      .getOrCreate()
  }

}
