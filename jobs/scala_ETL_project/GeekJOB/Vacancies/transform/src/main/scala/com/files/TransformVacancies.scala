package com.files

import EL.Extract.take
import EL.Load.give
import Spark.SparkApp
import com.Config.FolderName.FolderName
import com.Config.{FolderName, LocalConfig}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical.Repartition
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.rogach.scallop.ScallopOption

import scala.annotation.tailrec

object TransformVacancies extends App with SparkApp {

  private val conf = new LocalConfig(args, "hh") {
    val partitions: ScallopOption[Int] = opt[Int](default = Some(3), validate = _ > 0)

    define()
  }

  override val ss: SparkSession = defineSession(conf.fileConf)


  private val sc: SparkContext = ss.sparkContext

  val vacanciesRaw: RDD[String] = sc.textFile(conf.fileConf.fs.getPath(FolderName.Raw), conf.partitions())

  stopSpark()
}