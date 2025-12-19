package org.example.headhunter

import org.example.config.TableConfig.TableRegistry
import org.example.core.implement.HDFS.HDFSService
import org.example.core.implement.Network.{STTPBackendContext, STTPBackends, STTPService}
import org.example.core.implement.Postgres.PostgresService
import org.example.core.{ETLCycle, SparkApp}
import org.example.headhunter.config.{HHArgsLoader, HHFileLoader, HHFolderNames}
import org.example.headhunter.implement.{HHExtractor, HHTransformer}

object HeadHunterMain extends App {

  private val argsConfig = new HHArgsLoader(args)
  private val fileConfig = new HHFileLoader(argsConfig.common.confFile, argsConfig.common.saveFolder)

  private val spark = SparkApp.defineSession(fileConfig.structures.sparkConf, argsConfig.common.etlPart)

  private val dbService = new PostgresService(fileConfig.structures.dbConf)

  private val ec = new ETLCycle(
    spark,
    dbService,
    new HDFSService(fileConfig.structures.fsConf),
    new STTPService(fileConfig.structures.netConf, () => STTPBackendContext.getBackend(STTPBackends.DEFAULT))
  )

  private val extractor = new HHExtractor(
    dbService,
    fileConfig.common.apiBaseUrl,
    fileConfig.fieldIDs,
    argsConfig.dateFrom,
    fileConfig.pageLimit,
    fileConfig.vacsPerPage,
    fileConfig.structures.netConf.requestsPS,
    fileConfig.common.rawPartitions
  )

  ec.run(
    argsConfig.common.etlPart,
    extractor = Some(extractor),
    transformer = Some(new HHTransformer(fileConfig.common.transformPartitions)),
    loader = Some(() => Seq(
      HHFolderNames.employers.loadDefinition,
      TableRegistry.Vacancies,
      TableRegistry.Skills,
      HHFolderNames.languages.loadDefinition
    )),
    updater = Some(() => fileConfig.common.updateLimit)
  )

  spark.stop()
}
