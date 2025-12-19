package org.example.core.implement.Postgres

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.config.Cases.Structures.DBConf
import org.example.config.FolderName.FolderName
import org.example.core.Interfaces.Services.DataBaseService

class PostgresService(conf: DBConf) extends DataBaseService {

  override def load(spark: SparkSession, targetTable: String): DataFrame =
    PostgresUtils.loadHelper(spark, conf, Seq(("dbtable", targetTable)))

  override def getActiveVacancies(spark: SparkSession, limit: Int): DataFrame = {
    val query: String =
      s"SELECT id FROM ${conf.getDBTableName(FolderName.Vacancies)} WHERE closed_at is null ORDER BY published_at LIMIT $limit"
    PostgresUtils.loadHelper(spark, conf, Seq(("query", query)))
  }

  override def updateActiveVacancies(ids: DataFrame): Unit = {
    PostgresUtils.updateBulk(
      conf,
      ids,
      conf.getDBTableName(FolderName.Vacancies),
      "closed_at = now()::DATE"
    )
  }

  override def save(df: DataFrame, folderName: FolderName, conflicts: Seq[String],
                    updates: Option[Seq[String]]): Unit =
    PostgresUtils.save(conf, df, conf.getDBTableName(folderName), conflicts, updates)

}