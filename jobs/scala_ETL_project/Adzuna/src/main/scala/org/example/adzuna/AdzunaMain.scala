package org.example.adzuna

import org.example.adzuna.config.{AdzunaArgsLoader, AdzunaFileLoader}
import org.example.adzuna.implement.{AdzunaExtractor, AdzunaTransformer}
import org.example.config.FolderName.{CustomDomain, FolderName, Raw, Stage}
import org.example.config.TableConfig.TableRegistry
import org.example.core.implement.HDFS.HDFSService
import org.example.core.implement.Network.{STTPBackendContext, STTPBackends, STTPService}
import org.example.core.implement.Postgres.PostgresService
import org.example.core.objects.LoadDefinition
import org.example.core.{ETLCycle, SparkApp}

object AdzunaMain extends App {

  private val argsConfig = new AdzunaArgsLoader(args)
  private val fileConfig = new AdzunaFileLoader(argsConfig.common.confFile, argsConfig.common.saveFolder, argsConfig.locationIndex())

  private val spark = SparkApp.defineSession(fileConfig.structures.sparkConf, argsConfig.common.etlPart)

  private val etl = new ETLCycle(
    spark,
    new PostgresService(fileConfig.structures.dbConf),
    new HDFSService(fileConfig.structures.fsConf),
    new STTPService(fileConfig.structures.netConf, () => STTPBackendContext.getBackend(STTPBackends.DEFAULT))
  )

  private val extractor = new AdzunaExtractor(
    fileConfig.common.apiBaseUrl, fileConfig.apiParams, fileConfig.pageLimit,
    fileConfig.structures.netConf.requestsPS, fileConfig.common.rawPartitions
  )

  private val transformer = new AdzunaTransformer(
    fileConfig.currency, fileConfig.urlDomain, fileConfig.apiParams.locationTag,
    fileConfig.common.transformPartitions
  )

  etl.run(
    argsConfig.common.etlPart,
    Some(extractor),
    Some(transformer),
    Some(() => Seq(
      LoadDefinition(
        FolderName(Stage, CustomDomain(fileConfig.apiParams.locationTag), "Vacancies"),
        TableRegistry.Vacancies.config
      )
    )),
    rawFolder = FolderName(Raw, CustomDomain(fileConfig.apiParams.locationTag), "Vacancies")
  )

  spark.stop()
}
