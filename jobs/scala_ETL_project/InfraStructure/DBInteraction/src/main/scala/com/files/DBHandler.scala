package com.files

import com.files.Common.DBConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.Serializable
import java.sql.{Connection, DatabaseMetaData, DriverManager, PreparedStatement, ResultSet}

object DBHandler extends Serializable {


  private def tableExists(conn: Connection, tableName: String): Boolean = {
    val meta: DatabaseMetaData = conn.getMetaData
    val rs: ResultSet = meta.getTables(null, null, tableName.toLowerCase, Array[String]("TABLE"))
    val exists = rs.next()
    rs.close()
    exists
  }


  def save(
            df: DataFrame,
            conf: DBConf,
            folderName: FolderName,
            conflicts: Seq[String],
            updates: Option[Seq[String]]
          ): Unit = {
    val conn = DriverManager.getConnection(
      conf.url,
      conf.name,
      conf.pass
    )
    val tableName: String = conf.getDBTableName(folderName)

    val isExist: Boolean = tableExists(conn, tableName)
    conn.close()

    if (isExist) {
      saveWithOnConflict(
        df = df,
        conf = conf,
        tableName = tableName,
        conflicts = conflicts,
        updates = updates
      )
    } else {
      saveWithCreatingTable(
        df = df,
        conf = conf,
        tableName = tableName,
      )
    }
  }

  private def saveWithCreatingTable(
            df: DataFrame,
            conf: DBConf,
            tableName: String
          ): Unit = {
    df.write
      .format("jdbc")
      .option("truncate", value = true)
      .option("driver", "org.postgresql.Driver")
      .option("url", conf.url)
      .option("user", conf.name)
      .option("password", conf.pass)
      .option("dbtable", tableName)
      .save()
  }

  private def saveWithOnConflict(
            df: DataFrame,
            conf: DBConf,
            tableName: String,
            conflicts: Seq[String],
            updates: Option[Seq[String]]
          ): Unit = {

    val columns = df.columns
    val columnList = columns.mkString(", ")
    val placeholders = columns.map(_ => "?").mkString(", ")
    val conflictCols = conflicts.mkString(", ")
    val conflictAction = updates match {
      case Some(value) => s"do update set ${value.map(col => s"$col = EXCLUDED.$col").mkString(", ")}"
      case None => "do nothing"
    }

    val sql = s"""
        insert into $tableName ($columnList)
        values ($placeholders) on conflict ($conflictCols)
        $conflictAction
    """

    df.foreachPartition { (partition: Iterator[Row]) =>
      var conn: Connection = null
      var stmt: PreparedStatement = null

      try {
        conn = DriverManager.getConnection(
          conf.url,
          conf.name,
          conf.pass
        )
        stmt = conn.prepareStatement(sql)

        var batchSize = 0
        val batchLimit = 1000

        partition.foreach { row =>
          columns.zipWithIndex.foreach { case (col, idx) =>
            stmt.setObject(idx + 1, row.getAs[Any](col))
          }
          stmt.addBatch()
          batchSize += 1

          if (batchSize >= batchLimit) {
            stmt.executeBatch()
            batchSize = 0
          }
        }

        if (batchSize > 0) {
          stmt.executeBatch()
        }
      } finally {
        if (stmt != null) stmt.close()
        if (conn != null) conn.close()
      }

      ()
    }
  }



  private def loadHelper(
                        spark: SparkSession,
                        conf: DBConf,
                        option: Either[FolderName, String]
                        ): DataFrame = {

    val op: (String, String) = option match {
      case Left(name) => ("dbtable", conf.getDBTableName(name))
      case Right(query) => ("query", query)
    }

    spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", conf.url)
      .option("user", conf.name)
      .option("password", conf.pass)
      .option(op._1, op._2)
      .load()
  }

  def load(spark: SparkSession, conf: DBConf, folderName: FolderName): DataFrame =
    loadHelper(spark, conf, Left(folderName))

  def load(spark: SparkSession, conf: DBConf, query: String): DataFrame =
    loadHelper(spark, conf, Right(query))
}
