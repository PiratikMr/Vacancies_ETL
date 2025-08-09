package com.files

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.rogach.scallop.ScallopOption
import sttp.model.StatusCode

import java.sql.{Connection, DriverManager, PreparedStatement}

object UpdateVacancies extends App with SparkApp {

  private class Conf(args: Array[String]) extends LocalConfig(args) {
    lazy val limit: Int = getFromConfFile[Int]("updateLimit")
    val offset: ScallopOption[Int] = opt[Int](name = "offset")

    define()
  }

  private val conf: Conf = new Conf(args)
  private val spark: SparkSession = defineSession(conf.sparkConf, conf.urlConf.requestsPS)


  private val ids: DataFrame = DBHandler.load(spark, conf.dbConf,
    s"SELECT id FROM hh_vacancies WHERE is_active is true ORDER BY published_at LIMIT ${conf.limit} OFFSET ${conf.offset() * conf.limit}")
    .repartition(conf.urlConf.requestsPS)


  import spark.implicits._

  private val update: DataFrame = ids.mapPartitions(part => {
    part.flatMap(row => {
      val id: Long = row.getLong(0)
      val res = URLHandler.read(s"https://api.hh.ru/vacancies/$id", conf.urlConf)

      if (
        (res.isSuccess && res.body.right.get.contains(""""archived":true"""))
        || res.code.equals(StatusCode.NotFound)
      ) Some(id)
      else None
    })
  }).toDF("id")

  private val updateQuery: String = "UPDATE hh_vacancies SET is_active = false WHERE id = ?"

  update.foreachPartition { (partition: Iterator[Row]) =>
    var conn: Connection = null
    var stmt: PreparedStatement = null

    try {
      conn = DriverManager.getConnection(
        conf.dbConf.url,
        conf.dbConf.name,
        conf.dbConf.pass
      )
      conn.setAutoCommit(false)
      stmt = conn.prepareStatement(updateQuery)

      var batchSize = 0
      val batchLimit = 1000

      partition.foreach { row =>
        stmt.setLong(1, row.getLong(0))
        stmt.addBatch()
        batchSize += 1

        if (batchSize >= batchLimit) {
          stmt.executeBatch()
          conn.commit()
          batchSize = 0
        }
      }

      if (batchSize > 0) {
        stmt.executeBatch()
        conn.commit()
      }
    } catch {
      case e: Exception =>
        if (conn != null) conn.rollback()
        throw e
    } finally {
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
  }

  spark.stop()
}
