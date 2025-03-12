package com.files

import EL.Extract.take
import EL.Load.give
import Spark.SparkApp
import com.Config.LocalConfig
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, explode, to_timestamp, udf, unix_timestamp}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.ScallopOption

import scala.annotation.tailrec

object TransformVac extends App with SparkApp {

  private val conf = new LocalConfig(args) {
    val partitions: ScallopOption[Int] = opt[Int](default = Some(1), validate = _ > 0)

    define()
  }

  override val ss: SparkSession = defineSession(conf.fileConf.spark)

  private val areasMap: Map[Long, Option[Long]] = take(
    ss = ss,
    conf = conf.fileConf.fs,
    fileName = "areas",
    isRoot = true
  ).get
    .rdd
    .map(row => {
      val id: Long = row.getAs[Long]("id")
      val parent_id: Option[Long] = Option(row.getAs[Long]("parent_id"))
      (id, parent_id)
  })
    .collectAsMap()
    .toMap

  private def getCountry(id: Long): Long = {
    @tailrec
    def f(i: Long = id): Long = {
      areasMap.get(i).flatten match {
        case Some(v) => f(v)
        case None => i
      }
    }
    f()
  }
  private val udfCountry: UserDefinedFunction = udf((id: Long) => getCountry(id))


  private val vacanciesDF: DataFrame = take(
    ss = ss,
    conf = conf.fileConf.fs,
    fileName = conf.fileConf.fs.vacanciesRawFileName,
    isRoot = false
  ).get
    .withColumn("id", col("id").cast(LongType)) // id

    .withColumn("region_area_id", col("area").getField("id").cast(LongType))
    .withColumn("country_area_id", udfCountry(col("region_area_id")))
    .withColumn("close_to_metro", col("address").getField("metro_stations").isNotNull)

    .withColumn("salary_from", col("salary").getField("from"))
    .withColumn("salary_to", col("salary").getField("to"))
    .withColumn("currency_id", col("salary").getField("currency"))

    .withColumn("schedule_id", col("schedule").getField("id"))
    .withColumn("experience_id", col("experience").getField("id"))
    .withColumn("employment_id", col("employment").getField("id"))

    .withColumn("publish_date", to_timestamp(col("published_at"), "yyyy-MM-dd'T'HH:mm:ssZ"))

    .withColumn("roles", explode(col("professional_roles")))
    .withColumn("role_id", col("roles.id").cast(LongType))


    .select("id", "name", "region_area_id", "country_area_id", "salary_from", "salary_to", "close_to_metro", "publish_date",
    "schedule_id", "experience_id", "employment_id", "currency_id", "role_id")

    .dropDuplicates("id")

  give(
    conf = conf.fileConf.fs,
    fileName = conf.fileConf.fs.vacanciesTransformedFileName,
    data = vacanciesDF.repartition(conf.partitions()),
    isRoot = false
  )

  stopSpark()
}