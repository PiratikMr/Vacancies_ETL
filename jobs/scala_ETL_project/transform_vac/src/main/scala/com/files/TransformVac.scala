package com.files

import EL.Extract.take
import EL.Load.give
import Spark.SparkApp
import com.Config.HDFSConfig
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, explode, first, udf}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.DataFrame
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.annotation.tailrec

object TransformVac extends App with SparkApp {

  private val conf = new ScallopConf(args) {
    val vacanciesInFileName: ScallopOption[String] = opt[String](default = Some(HDFSConfig.vacanciesRawFileName))
    val vacanciesOutFileName: ScallopOption[String] = opt[String](default = Some(HDFSConfig.vacanciesTransformedFileName))

    val partitions: ScallopOption[Int] = opt[Int](default = Some(1), validate = _ > 0)

    verify()
  }

  private val areasMap: Map[Long, Option[Long]] = take(isRoot = true, fileName = "areas").get
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


  private val vacanciesDF: DataFrame = take(isRoot = false, fileName = conf.vacanciesInFileName()).get

    .withColumn("id", col("id").cast(LongType))

    .withColumn("region_area_id", col("area").getField("id").cast(LongType))
    .withColumn("country_area_id", udfCountry(col("region_area_id")))

    .withColumn("salary_from", col("salary").getField("from"))
    .withColumn("salary_to", col("salary").getField("to"))

    .withColumn("close_to_metro", col("address").getField("metro_stations").isNotNull)

    .withColumn("schedule_id", col("schedule").getField("id"))
    .withColumn("experience_id", col("experience").getField("id"))
    .withColumn("employment_id", col("employment").getField("id"))
    .withColumn("currency_id", col("salary").getField("currency"))

    .withColumn("roles", explode(col("professional_roles")))
    .withColumn("role_id", col("roles.id").cast(LongType))


    .select("id", "name", "region_area_id", "country_area_id", "salary_from", "salary_to", "close_to_metro",
    "schedule_id", "experience_id", "employment_id", "currency_id", "role_id")

    .dropDuplicates("id")

  give(isRoot = false, data = vacanciesDF.repartition(conf.partitions()), fileName = conf.vacanciesOutFileName())
  stopSpark()
}