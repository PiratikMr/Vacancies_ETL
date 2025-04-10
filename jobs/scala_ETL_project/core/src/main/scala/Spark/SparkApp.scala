package Spark

import com.Config.ProjectConfig
import org.apache.spark.sql.SparkSession

trait SparkApp extends Serializable {

  val ss: SparkSession

  def defineSession(conf: ProjectConfig): SparkSession = {
    SparkSession.builder()
      .appName(conf.spark.name)
      .master(conf.spark.master)
      .getOrCreate()
  }

  def stopSpark(): Unit = ss.stop()
}
