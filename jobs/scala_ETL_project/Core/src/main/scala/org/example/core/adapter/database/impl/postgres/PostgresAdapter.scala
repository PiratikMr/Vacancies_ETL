package org.example.core.adapter.database.impl.postgres

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.model.structures.DBConf

class PostgresAdapter(conf: DBConf) extends DataBaseAdapter {

  override def loadTable(
                          spark: SparkSession,
                          targetTable: String
                        ): DataFrame =
    PostgresUtils.loadTable(spark, conf, targetTable)


  override def loadQuery(
                              spark: SparkSession,
                              query: String
                            ): DataFrame =
    PostgresUtils.loadQuery(spark, conf, query)


  override def save(
                     df: DataFrame,
                     targetTable: String,
                     conflicts: Seq[String],
                     updates: Option[Seq[String]] = None
                   ): Unit = {
    PostgresUtils.save(conf, df, targetTable, conflicts, updates)
  }

  override def saveWithReturn(
                               spark: SparkSession,
                               df: DataFrame,
                               targetTable: String,
                               returns: Seq[String],
                               conflicts: Seq[String],
                               updates: Option[Seq[String]] = None
                             ): DataFrame = {
    PostgresUtils.saveWithReturn(spark, conf, df, targetTable, returns, conflicts, updates)
  }
}