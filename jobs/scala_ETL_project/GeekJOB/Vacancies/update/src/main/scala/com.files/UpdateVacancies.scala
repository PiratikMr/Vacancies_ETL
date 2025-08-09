package com.files

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.rogach.scallop.ScallopOption

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
    s"SELECT id FROM gj_vacancies WHERE is_active is true ORDER BY published_at LIMIT ${conf.limit} OFFSET ${conf.offset() * conf.limit}")
    .repartition(conf.urlConf.requestsPS)


  import spark.implicits._

  private val update: DataFrame = ids.mapPartitions(part => {
    part.flatMap(row => {
      val id = row.getString(0)
      URLHandler.readOrNone(s"https://geekjob.ru/vacancy/$id", conf.urlConf) match {
        case Some(body) if body.contains("Эта вакансия была перемещена в архив.") => Some(id)
        case _ => None
      }
    })
  }).toDF("id").repartition(conf.urlConf.requestsPS)

  val updateQuery = """
    UPDATE gj_vacancies
    SET is_active = false
    WHERE id = ?
  """

  update.foreachPartition { (partition: Iterator[Row]) =>
    var conn: Connection = null
    var stmt: PreparedStatement = null

    try {
      conn = DriverManager.getConnection(
        conf.dbConf.url,
        conf.dbConf.name,
        conf.dbConf.pass
      )
      conn.setAutoCommit(false) // Отключаем авто-коммит для пакетной обработки
      stmt = conn.prepareStatement(updateQuery)

      var batchSize = 0
      val batchLimit = 1000

      partition.foreach { row =>
        stmt.setString(1, row.getString(0)) // Предполагаем, что ID находится в первом столбце
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
