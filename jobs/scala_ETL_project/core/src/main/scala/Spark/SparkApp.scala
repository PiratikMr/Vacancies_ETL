package Spark

import com.Config.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkApp extends Serializable {

  val ss: SparkSession

  def defineSession(conf: SparkConf): SparkSession = {
    SparkSession.builder()
      .appName(conf.name)
      .master(conf.master)
      .getOrCreate()
  }

  def stopSpark(): Unit = ss.stop()
}
