package org.example.headhunter.dictionaries.implement

import org.apache.spark.sql.functions.{col, count, explode, when}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import org.example.config.FolderName.{Dictionaries, FolderName, Stage}
import org.example.core.Interfaces.ETL.Transformer
import org.example.headhunter.dictionaries.config.HHFolderNames

import scala.annotation.tailrec

object AreasTransformer extends Transformer {

  override def toRows(spark: SparkSession, rawDS: Dataset[String]): DataFrame =
    spark.read.json(rawDS)

  override def transform(spark: SparkSession, rawDF: DataFrame): Map[FolderName, DataFrame] =
    {
      import spark.implicits._

      val flatAreas: DataFrame = {
        @tailrec
        def flattenAreas(acc: DataFrame = rawDF.select("id", "name", "parent_id"), current: DataFrame = rawDF): DataFrame = {
          if (current.agg(count(when(functions.size(col("areas")).gt(0), 1))).first().getLong(0) == 0) {
            acc.withColumn("id", col("id").cast(LongType)).withColumn("parent_id", col("parent_id").cast(LongType))
          } else {
            val next = current.withColumn("areas", explode(col("areas"))).select("areas.*").select("id", "name", "parent_id", "areas")
            flattenAreas(acc.union(next.drop("areas")), next)
          }
        }
        flattenAreas()
      }

      val areasMap: Map[Long,(String, Option[Long])] = flatAreas.rdd
        .map(row => {
          val id: Long = row.getAs[Long]("id")
          val name: String = row.getAs[String]("name")
          val parent_id: Option[Long] = Option(row.getAs[Long]("parent_id"))

          (id, (name, parent_id))
        })
        .collectAsMap()
        .toMap

      val areasDF: DataFrame = areasMap.map { case (i, (name, p)) =>
          @tailrec
          def loop(id: Long, prev: Option[Long], parent: Option[Long]): (Long, Option[Long]) = {
            parent match {
              case Some(value) => loop(id, Some(value), areasMap(value)._2)
              case None => (id, prev)
            }
          }

          val (id, country_id) = loop(i, p, p)
          (id, name, country_id)

        }.toSeq
        .toDF("id", "name", "parent_id")
        .dropDuplicates("id")

      Map(HHFolderNames.areas.stage -> areasDF)
    }
}
