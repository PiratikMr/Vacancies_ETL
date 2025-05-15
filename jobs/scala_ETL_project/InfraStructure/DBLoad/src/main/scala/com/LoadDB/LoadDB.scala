package com.LoadDB

import com.Config.CommonConfig
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.Serializable
import java.sql.{Connection, DatabaseMetaData, DriverManager, PreparedStatement, ResultSet}

object LoadDB extends Serializable {

  private def tableExists(conn: Connection, tableName: String): Boolean = {
    val meta: DatabaseMetaData = conn.getMetaData
    val rs: ResultSet = meta.getTables(null, null, tableName.toLowerCase, Array[String]("TABLE"))
    val exists = rs.next()
    rs.close()
    exists
  }


  def save(
            conf: CommonConfig,
            data: DataFrame,
            tableName: String,
            conflicts: Seq[String],
            updates: Seq[String] = null
          ): Unit = {
    val conn = DriverManager.getConnection(
      conf.db.DBurl,
      conf.db.userName,
      conf.db.userPassword
    )
    val isExist: Boolean = tableExists(conn, tableName)
    conn.close()

    if (isExist) {
      load(
        conf = conf,
        data = data,
        tableName = tableName,
        conflicts = conflicts,
        updates = updates
      )
    } else {
      give(
        conf = conf,
        data = data,
        tableName = tableName,
      )
    }
  }

  private def give(
            conf: CommonConfig,
            data: DataFrame,
            tableName: String,
          ): Unit = {
    data.write
      .format("jdbc")
      .option("truncate", value = true)
      .option("driver", "org.postgresql.Driver")
      .option("url", conf.db.DBurl)
      .option("user", conf.db.userName)
      .option("password", conf.db.userPassword)
      .option("dbtable", tableName)
      .save()
  }

  private def load(
          conf: CommonConfig,
          data: DataFrame,
          tableName: String,
          conflicts: Seq[String],
          updates: Seq[String]
          ): Unit = {

    val columns = data.columns
    val columnList = columns.mkString(", ")
    val placeholders = columns.map(_ => "?").mkString(", ")
    val conflictCols = conflicts.mkString(", ")
    val conflictAction = if (updates != null) {
      s"do update set ${updates.map(col => s"$col = EXCLUDED.$col").mkString(", ")}"
    } else {
      "do nothing"
    }

    val sql = s"""
        insert into $tableName ($columnList)
        values ($placeholders) on conflict ($conflictCols)
        $conflictAction
    """

    data.foreachPartition { (partition: Iterator[Row]) =>
      var conn: Connection = null
      var stmt: PreparedStatement = null

      try {
        conn = DriverManager.getConnection(
          conf.db.DBurl,
          conf.db.userName,
          conf.db.userPassword
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




  def take(
            ss: SparkSession,
            conf: CommonConfig,
            tableName: String
          ): DataFrame = {
    ss.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", conf.db.DBurl)
      .option("user", conf.db.userName)
      .option("password", conf.db.userPassword)
      .option("dbtable", tableName)
      .load()
  }
}
