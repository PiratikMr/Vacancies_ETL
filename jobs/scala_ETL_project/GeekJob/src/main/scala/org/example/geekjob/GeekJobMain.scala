package org.example.geekjob

import org.example.config.FolderName.{FolderName, Stage}
import org.example.config.TableConfig.{TableConfig, TableRegistry}
import org.example.core.implement.HDFS.HDFSService
import org.example.core.implement.Network.{STTPBackendContext, STTPBackends, STTPService}
import org.example.core.implement.Postgres.PostgresService
import org.example.core.objects.LoadDefinition
import org.example.core.{ETLCycle, SparkApp}
import org.example.geekjob.config.{GeekJobArgsLoader, GeekJobFileLoader, GeekJobFolderNames}
import org.example.geekjob.implement.{GeekJobExtractor, GeekJobTransformer}

object GeekJobMain extends App {

  private val argsConfig = new GeekJobArgsLoader(args)
  private val fileConfig = new GeekJobFileLoader(argsConfig.common.confFile, argsConfig.common.saveFolder)

  private val spark = SparkApp.defineSession(fileConfig.structures.sparkConf, argsConfig.common.etlPart)


  private val ec = new ETLCycle(
    spark,
    new PostgresService(fileConfig.structures.dbConf),
    new HDFSService(fileConfig.structures.fsConf),
    new STTPService(fileConfig.structures.netConf, () => STTPBackendContext.getBackend(STTPBackends.UNSAFE))
  )

  private val extractor = new GeekJobExtractor(
    fileConfig.common.apiBaseUrl,
    fileConfig.pageLimit,
    fileConfig.structures.netConf.requestsPS,
    fileConfig.common.rawPartitions
  )

  private val transformer = new GeekJobTransformer(
    argsConfig.common.saveFolder,
    fileConfig.common.transformPartitions
  )

  ec.run(
    argsConfig.common.etlPart,
    extractor = Some(extractor),
    transformer = Some(transformer),
    loader = Some(() => Seq(
      TableRegistry.Vacancies,
      TableRegistry.Fields,
      LoadDefinition(GeekJobFolderNames.jobFormats, TableConfig.list()),
      LoadDefinition(GeekJobFolderNames.grades, TableConfig.list()),
      TableRegistry.Locations,
      TableRegistry.Skills
    )),
    updater = Some(() => fileConfig.common.updateLimit)
  )

  spark.stop()
}
