package org.example.core.implement.Postgres

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.example.config.Cases.Structures.DBConf

import java.sql.{Connection, DatabaseMetaData, DriverManager}
import java.util.UUID

object PostgresUtils {

  def loadHelper(spark: SparkSession, conf: DBConf, options: Seq[(String, String)]): DataFrame =
  {
    val dfReader = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", conf.url)
      .option("user", conf.name)
      .option("password", conf.pass)
    options.foldLeft(dfReader)((reader, option) => reader.option(option._1, option._2))
      .load()
  }

  def save(conf: DBConf,
           df: DataFrame,
           targetTable: String,
           conflicts: Seq[String],
           updates: Option[Seq[String]] = None): Unit =
  {
    if (doesTableExist(conf, targetTable))
      saveUpsert(conf, df, targetTable, conflicts, updates)
    else
      saveOverwrite(conf, df, targetTable)
  }

  def updateBulk(conf: DBConf,
                 df: DataFrame,
                 targetTable: String,
                 setClause: String): Unit =
  {
    val joinColumn: String = df.columns(0)

    bulkMerge(conf, df, targetTable, (stagingTable: String) => {
      s"""
         |UPDATE $targetTable t
         |SET $setClause
         |FROM $stagingTable s
         |WHERE t.$joinColumn = s.id
         |""".stripMargin
    })
  }


  private def buildMergeQuery(
                               targetTable: String,
                               sourceTable: String,
                               columns: Seq[String],
                               conflicts: Seq[String],
                               updates: Option[Seq[String]]
                             ): String =
  {
    val cols = columns.mkString(", ")
    val conflict = conflicts.mkString(", ")

    val conflictAction = updates match {
      case Some(updCols) =>
        val sets = updCols.map(c => s"$c = EXCLUDED.$c").mkString(", ")
        s"DO UPDATE SET $sets"
      case None =>
        "DO NOTHING"
    }

    s"""
       |INSERT INTO $targetTable ($cols)
       |SELECT $cols FROM $sourceTable
       |ON CONFLICT ($conflict) $conflictAction
       |""".stripMargin
  }


  private def saveUpsert(conf: DBConf, df: DataFrame, targetTable: String, conflicts: Seq[String],
                         updates: Option[Seq[String]]): Unit =
  {
    bulkMerge(conf, df, targetTable, (stagingTable: String) =>
      buildMergeQuery(targetTable, stagingTable, df.columns, conflicts, updates)
    )
  }

  private def bulkMerge(conf: DBConf, df: DataFrame, targetTable: String,
                        sqlBuilder: String => String): Unit =
  {
    val stagingTable = s"${targetTable}_staging_${UUID.randomUUID().toString.replace("-", "")}"
    try {
      saveOverwrite(conf, df, stagingTable)

      val sql = sqlBuilder(stagingTable)
      executeSql(conf, sql)
    } finally {
      executeSql(conf, s"DROP TABLE IF EXISTS $stagingTable")
    }
  }


  private def getConnection(conf: DBConf): Connection =
    DriverManager.getConnection(conf.url, conf.name, conf.pass)


  private def saveOverwrite(conf: DBConf, df: DataFrame, targetTable: String): Unit =
  {
    df.write
      .format("jdbc")
      .mode(SaveMode.Overwrite)
      .option("truncate", "true")
      .option("driver", "org.postgresql.Driver")
      .option("url", conf.url)
      .option("user", conf.name)
      .option("password", conf.pass)
      .option("dbtable", targetTable)
      .save()
  }

  private def executeSql(conf: DBConf, sql: String): Unit =
  {
    val conn = getConnection(conf)
    try {
      val stmt = conn.createStatement()
      stmt.execute(sql)
      stmt.close()
    } finally {
      conn.close()
    }
  }

  private def doesTableExist(conf: DBConf, targetTable: String): Boolean =
  {
    val conn = getConnection(conf)
    try {
      val meta: DatabaseMetaData = conn.getMetaData
      val rs = meta.getTables(null, null, targetTable, Array[String]("TABLE"))
      val exists = rs.next()
      rs.close()

      exists
    } finally {
      conn.close()
    }
  }
}
