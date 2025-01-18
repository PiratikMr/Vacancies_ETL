package Spark

import org.apache.spark.sql.SparkSession
import com.Config.SparkConfig

trait SparkApp extends Serializable {
  val ss: SparkSession = SparkSession.builder()
    .appName(SparkConfig.name).master(SparkConfig.master).getOrCreate()

  def stopSpark(): Unit = {
    ss.stop()
  }
}
