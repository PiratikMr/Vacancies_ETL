package org.example.core.adapter.database.impl.postgres

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.typesafe.scalalogging.LazyLogging
import org.example.core.config.model.structures.DBConf

import java.sql.{Connection, DriverManager}
import java.util.UUID
import scala.util.Try

object PostgresUtils extends LazyLogging {


  def loadTable(spark: SparkSession, conf: DBConf, targetTable: String): DataFrame = {
    readJdbc(spark, conf, "dbtable", targetTable)
  }

  def loadQuery(spark: SparkSession, conf: DBConf, query: String): DataFrame = {
    logger.debug(s"Выполнение SQL запроса:\n$query")

    readJdbc(spark, conf, "query", query)
  }

  def save(conf: DBConf,
           df: DataFrame,
           targetTable: String,
           conflicts: Seq[String],
           updates: Option[Seq[String]]): Unit = {

    withStaging(conf, df.dropDuplicates(conflicts)) { stagingTable =>
      executeInTransaction(conf) { conn =>
        val sql = buildUpsertQuery(targetTable, stagingTable, df.columns, conflicts, updates)
        executeSql(conn, sql)
      }
    }
  }

  def saveWithReturn(
                      spark: SparkSession,
                      conf: DBConf,
                      df: DataFrame,
                      targetTable: String,
                      returns: Seq[String],
                      conflicts: Seq[String],
                      updates: Option[Seq[String]]
                    ): DataFrame = {

    withStaging(conf, df.dropDuplicates(conflicts)) { stagingTable =>
      executeInTransaction(conf) { conn =>
        val sql = buildUpsertQuery(targetTable, stagingTable, df.columns, conflicts, updates)
        executeSql(conn, sql)
      }

      val joinCondition = conflicts.map(c => s"t.$c = s.$c").mkString(" AND ")
      val returnCols = returns.map(c => s"t.$c").mkString(", ")

      val result = loadQuery(spark, conf,
        s"""
           |SELECT $returnCols
           |FROM $targetTable as t
           |JOIN $stagingTable as s ON $joinCondition
           |""".stripMargin
      )

      result.localCheckpoint()
    }
  }


  private def withStaging[T](conf: DBConf, df: DataFrame)(block: String => T): T = {
    val stagingTable = s"staging_${UUID.randomUUID().toString.replace("-", "")}"
    try {
      writeDefault(conf, df, stagingTable)
      block(stagingTable)
    } finally {
      Try(dropTable(conf, stagingTable))
    }
  }

  private def executeInTransaction[T](conf: DBConf)(block: Connection => T): T = {
    val conn = getConnection(conf)
    try {
      conn.setAutoCommit(false)
      val result = block(conn)
      conn.commit()
      result
    } catch {
      case e: Throwable =>
        Try(conn.rollback())
        throw e
    } finally {
      conn.close()
    }
  }

  private def dropTable(conf: DBConf, tableName: String): Unit = {
    val conn = getConnection(conf)
    try {
      executeSql(conn, s"DROP TABLE IF EXISTS $tableName")
    } finally {
      conn.close()
    }
  }

  private def executeSql(conn: Connection, sql: String): Unit = {
    val stmt = conn.createStatement()
    try {
      logger.debug(s"Выполнение SQL команды:\n$sql")

      stmt.execute(sql)
    } finally {
      stmt.close()
    }
  }


  private def buildUpsertQuery(
                                target: String,
                                source: String,
                                columns: Seq[String],
                                conflicts: Seq[String],
                                updates: Option[Seq[String]]
                              ): String = {

    val colStr = columns.mkString(", ")

    val conflictColStr = conflicts.mkString(", ")

    val conflictAction = updates match {
      case Some(updCols) if updCols.nonEmpty =>
        val setClause = updCols.map(c => s"$c = EXCLUDED.$c").mkString(", ")
        s"DO UPDATE SET $setClause"
      case _ =>
        "DO NOTHING"
    }

    s"""
       |INSERT INTO $target ($colStr)
       |SELECT $colStr FROM $source
       |ON CONFLICT ($conflictColStr)
       |$conflictAction
       |""".stripMargin
  }


  private def baseOptions(conf: DBConf): Map[String, String] = Map(
    "driver" -> "org.postgresql.Driver",
    "url" -> conf.url,
    "user" -> conf.name,
    "password" -> conf.pass
  )

  private val JDBC = "jdbc"

  private def readJdbc(spark: SparkSession, conf: DBConf, key: String, value: String): DataFrame = {
    spark.read
      .format(JDBC)
      .options(baseOptions(conf))
      .option(key, value)
      .load()
  }

  private def writeDefault(conf: DBConf, df: DataFrame, targetTable: String): Unit = {
    df.write
      .format(JDBC)
      .mode(SaveMode.Append)
      .options(baseOptions(conf))
      .option("dbtable", targetTable)
      .option("batchsize", conf.batchSize.toString)
      .option("numPartitions", conf.maxPartitions.toString)
      .save()
  }

  private def getConnection(conf: DBConf): Connection =
    DriverManager.getConnection(conf.url, conf.name, conf.pass)
}