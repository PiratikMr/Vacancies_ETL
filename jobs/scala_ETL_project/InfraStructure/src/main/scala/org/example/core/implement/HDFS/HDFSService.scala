package org.example.core.implement.HDFS

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.example.config.Cases.Structures.FSConf
import org.example.config.FolderName.FolderName
import org.example.core.Interfaces.Services.StorageService

class HDFSService(conf: FSConf) extends StorageService {

  override def read(spark: SparkSession, folderName: FolderName): DataFrame =
    spark.read.parquet(conf.getPath(folderName))

  override def readText(spark: SparkSession, folderName: FolderName): Dataset[String] =
    spark.read.textFile(conf.getPath(folderName))

  override def write(df: DataFrame, folderName: FolderName): Unit =
    df.write.mode(SaveMode.Overwrite).parquet(conf.getPath(folderName))

  override def writeText(ds: Dataset[String], folderName: FolderName): Unit =
    ds.write.mode(SaveMode.Overwrite).text(conf.getPath(folderName))
}
