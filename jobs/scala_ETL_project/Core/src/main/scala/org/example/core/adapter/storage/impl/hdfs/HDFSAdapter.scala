package org.example.core.adapter.storage.impl.hdfs

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.example.core.adapter.storage.StorageAdapter
import org.example.core.config.model.structures.FSConf

class HDFSAdapter(conf: FSConf) extends StorageAdapter {

  override def read(spark: SparkSession, folderName: String, withDate: Boolean = true): DataFrame =
    spark.read.parquet(conf.getPath(folderName, withDate))

  override def readText(spark: SparkSession, folderName: String): Dataset[String] =
    spark.read.textFile(conf.getPath(folderName))

  override def write(df: DataFrame, folderName: String, withDate: Boolean = true): Unit =
    df.write.mode(SaveMode.Overwrite).parquet(conf.getPath(folderName, withDate))

  override def writeText(ds: Dataset[String], folderName: String): Unit =
    ds.write.mode(SaveMode.Overwrite).text(conf.getPath(folderName))
}
