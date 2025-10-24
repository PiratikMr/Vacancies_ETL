package com.files

import com.files.Common.DBConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.io.Serializable
import java.sql.{Connection, DatabaseMetaData, DriverManager, PreparedStatement, ResultSet}

object DBHandler extends Serializable {


  def updateActiveVacancies[T](conf: DBConf, dataset: Dataset[T]): Unit = {

    val query: String = s"UPDATE ${conf.platform}_vacancies SET is_active = false WHERE id = ?"

    dataset.foreachPartition { (partition: Iterator[T]) =>
      executeBatch[T](
        conf,
        query,
        partition,
        (st, value) => value match { case s: String => st.setString(1, s) case l: Long => st.setLong(1, l) },
        transactional = true
      )
    }
  }


  def load(spark: SparkSession, conf: DBConf, folderName: FolderName): DataFrame =
    loadHelper(spark, conf, Left(folderName))

  def load(spark: SparkSession, conf: DBConf, query: String): DataFrame =
    loadHelper(spark, conf, Right(query))


  def save(
            conf: DBConf, df: DataFrame, folderName: FolderName,
            conflicts: Option[Seq[String]] = None, updates: Option[Seq[String]] = None
          ): Unit = {

    if (does_tableExist(conf, folderName)) saveOnConflict(conf, df, folderName, conflicts, updates)
    else saveDefault(conf, df, folderName)
  }




  private def executeBatch[T](conf: DBConf,
                              query: String,
                              data: Iterator[T],
                              setParameters: (PreparedStatement, T) => Unit,
                              transactional: Boolean = false,
                              batchLimit: Int = 1000
                             ): Unit = {
    val conn: Connection = getConnection(conf)
    val stmt: PreparedStatement = conn.prepareStatement(query)

    try {
      if (transactional) conn.setAutoCommit(false)

      val tmp: Int = data.foldLeft(0)((batchSize, item) => {
        setParameters(stmt, item)
        stmt.addBatch()

        if (batchSize + 1 >= batchLimit) {
          stmt.executeBatch()
          if (transactional) conn.commit()
          -batchSize
        } else 1
      })

      if (tmp > 0) {
        stmt.executeBatch()
        if (transactional) conn.commit()
      }

    } catch {
      case e: Exception =>
        if (transactional) conn.rollback()
        throw e
    } finally {
      stmt.close()
      conn.close()
    }
  }

  private def getConnection(conf: DBConf): Connection = DriverManager.getConnection(conf.url, conf.name, conf.pass)


  private def does_tableExist(conf: DBConf, folderName: FolderName): Boolean = {
    val connection: Connection = getConnection(conf)
    val meta: DatabaseMetaData = connection.getMetaData
    val rs: ResultSet = meta.getTables(null, null, conf.getDBTableName(folderName), Array[String]("TABLE"))
    val exists: Boolean = rs.next()
    rs.close()
    connection.close()
    exists
  }




  private def saveDefault(conf: DBConf, df: DataFrame, folderName: FolderName): Unit = {
    df.write
      .format("jdbc")
      .option("truncate", value = true)
      .option("driver", "org.postgresql.Driver")
      .option("url", conf.url)
      .option("user", conf.name)
      .option("password", conf.pass)
      .option("dbtable", conf.getDBTableName(folderName))
      .save()
  }

  private def saveOnConflict(conf: DBConf, df: DataFrame, folderName: FolderName,
                              conflicts: Option[Seq[String]], updates: Option[Seq[String]]
          ): Unit = {

    val onConflict: String = conflicts match {
      case Some(value) =>
        val conflictAction: String = updates match {
          case Some(value) => s"do update set ${value.map(col => s"$col = EXCLUDED.$col").mkString(", ")}"
          case None => "do nothing"
        }

        s" on conflict (${value.mkString(", ")}) $conflictAction"
      case None => ""
    }

    val columns: Array[String] = df.columns

    val query: String = s"""
        insert into ${conf.getDBTableName(folderName)} (${columns.mkString(", ")})
        values (${columns.map(_ => "?").mkString(", ")})$onConflict
    """

    df.foreachPartition((part: Iterator[Row]) => executeBatch[Row](
      conf,
      query,
      part,
      (st, row: Row) => columns.zipWithIndex.foreach { case (col, idx) => st.setObject(idx + 1, row.getAs[Any](col)) }
    ))
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
}
