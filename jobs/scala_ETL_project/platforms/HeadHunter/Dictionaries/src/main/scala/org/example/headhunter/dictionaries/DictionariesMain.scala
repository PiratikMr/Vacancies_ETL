package org.example.headhunter.dictionaries

import org.apache.spark.sql.{Dataset, SparkSession}
import org.example.core.adapter.storage.impl.hdfs.HDFSAdapter
import org.example.core.adapter.web.WebAdapter
import org.example.core.adapter.web.impl.sttp.STTPAdapter
import org.example.core.config.model.structures.SparkConf
import org.example.core.util.SparkJob
import org.example.headhunter.config.FolderNames
import org.example.headhunter.dictionaries.config.{HHArgsLoader, HHFileLoader}
import org.example.headhunter.dictionaries.implement.{AreasTransformer, RolesTransformer}

object DictionariesMain {

  def main(args: Array[String]): Unit = {

    val argsConfig = new HHArgsLoader(args)
    val fileConfig = new HHFileLoader(argsConfig.common.confFile)

    val sparkJob = new SparkJob {
      override def sparkConf: SparkConf = fileConfig.structures.sparkConf

      override def sparkName: String = "dictionaries"
    }
    val spark = sparkJob.spark

    val hdfsService = new HDFSAdapter(fileConfig.structures.fsConf)
    val sttpService = STTPAdapter(fileConfig.structures.netConf)

    val rawAreas = extract(spark, sttpService, getUrl(fileConfig.common.apiBaseUrl, "/areas"))
    val areas = AreasTransformer.transform(spark, rawAreas)
    hdfsService.write(areas, FolderNames.areas, withDate = false)

    val rawRoles = extract(spark, sttpService, getUrl(fileConfig.common.apiBaseUrl, "/professional_roles"))
    val roles = RolesTransformer.transform(spark, rawRoles)
    hdfsService.write(roles, FolderNames.roles, withDate = false)
  }


  private def extract(spark: SparkSession, webService: WebAdapter, url: String): Dataset[String] = {
    import spark.implicits._

    val body = webService.readBody(url).getOrElse("")
    Seq(body).toDS
  }

  private def getUrl(api: String, endPoint: String): String =
    s"$api$endPoint"

}
