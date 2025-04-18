package com.files

import EL.Extract.take
import EL.Load.give
import Spark.SparkApp
import com.Config.FolderName.FolderName
import com.Config.{FolderName, LocalConfig}
import org.apache.spark.sql.catalyst.plans.logical.Repartition
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.ScallopOption

import scala.annotation.tailrec

object TransformVacancies extends App with SparkApp {

  private val conf = new LocalConfig(args) {
    val partitions: ScallopOption[Int] = opt[Int](default = Some(1), validate = _ > 0)

    define()
  }

  override val ss: SparkSession = defineSession(conf.fileConf)

  private val areasMap: Map[Long, Option[Long]] = take(
    ss = ss,
    conf = conf.fileConf,
    folderName = FolderName.Dict(FolderName.Areas)
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


  private val rawVac: DataFrame = take(
    ss = ss,
    conf = conf.fileConf,
    folderName = FolderName.Raw
  ).get

  private val genVac: DataFrame = rawVac
    .withColumn("id", col("id").cast(LongType))

    .withColumn("region_area_id", col("area").getField("id").cast(LongType))
    .withColumn("country_area_id", udfCountry(col("region_area_id")))
    .withColumn("close_to_metro", col("address").getField("metro_stations").isNotNull)

    .withColumn("salary_from", col("salary").getField("from"))
    .withColumn("salary_to", col("salary").getField("to"))
    .withColumn("currency_id", col("salary").getField("currency"))

    .withColumn("employer_id", col("employer").getField("id").cast(LongType))
    .withColumn("employer_name", col("employer").getField("name"))

    .withColumn("skills",
      if (rawVac.columns.contains("key_skills")) { col("key_skills") }
      else { lit(Array.empty[String]) }
    )

    .withColumn("schedule_id", col("schedule").getField("id"))
    .withColumn("experience_id", col("experience").getField("id"))
    .withColumn("employment_id", col("employment").getField("id"))

    .withColumn("publish_date", to_timestamp(col("published_at"), "yyyy-MM-dd'T'HH:mm:ssZ"))

    .withColumn("roles", explode(col("professional_roles")))
    .withColumn("role_id", col("roles.id").cast(LongType))

    .dropDuplicates("id")

  private val transformedVac: DataFrame = genVac
    .select("id", "name", "region_area_id", "country_area_id", "salary_from", "salary_to", "close_to_metro", "publish_date",
      "schedule_id", "experience_id", "employment_id", "employer_id", "currency_id", "role_id")

  private val employers: DataFrame = genVac
    .select(col("employer_id").as("id"), col("employer_name").as("name"))
    .filter(col("id").isNotNull)
    .dropDuplicates("id")

  private val skills: DataFrame = genVac
    .select(col("id"), explode(col("skills")).as("name"))


  // skills
  save(FolderName.Skills, skills)

  // employers
  save(FolderName.Employer, employers)

  // vacancies
  save(FolderName.Vac, transformedVac, conf.partitions())

  stopSpark()

  private def save(folderName: FolderName, dataFrame: DataFrame, repartition: Integer = 1): Unit = {
    give(
      conf = conf.fileConf,
      data = dataFrame.repartition(repartition),
      folderName = folderName
    )
  }
}