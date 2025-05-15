package Spark

import com.Config.CommonConfig
import org.apache.spark.sql.SparkSession

trait SparkApp extends Serializable {

  val ss: SparkSession

  def defineSession(conf: CommonConfig): SparkSession = {
    SparkSession.builder()
      .appName(conf.spark.name)
      .master(conf.spark.master)
      .getOrCreate()
  }

  def stopSpark(): Unit = ss.stop()
}
