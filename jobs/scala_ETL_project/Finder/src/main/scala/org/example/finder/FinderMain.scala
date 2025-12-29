package org.example.finder

import org.example.config.TableConfig.TableRegistry
import org.example.core.implement.HDFS.HDFSService
import org.example.core.implement.Network.{STTPBackendContext, STTPBackends, STTPService}
import org.example.core.implement.Postgres.PostgresService
import org.example.core.{ETLCycle, SparkApp}
import org.example.finder.implement.{FinderExtractor, FinderTransformer}
import org.example.finder.config.{FinderArgsLoader, FinderFileLoader}

object FinderMain extends App {

  private val argsConfig = new FinderArgsLoader(args)
  private val fileConfig = new FinderFileLoader(argsConfig.common.confFile, argsConfig.common.saveFolder)

  private val spark = SparkApp.defineSession(fileConfig.structures.sparkConf, argsConfig.common.etlPart)

  private val pc = new ETLCycle(
    spark,
    new PostgresService(fileConfig.structures.dbConf),
    new HDFSService(fileConfig.structures.fsConf),
    new STTPService(fileConfig.structures.netConf, () => STTPBackendContext.getBackend(STTPBackends.DEFAULT))
  )

  private val extractor = new FinderExtractor(fileConfig.finder, fileConfig.common.apiBaseUrl,
    fileConfig.structures.netConf.requestsPS, fileConfig.common.rawPartitions)
  private val transformer = new FinderTransformer(fileConfig.common.transformPartitions)

  pc.run(
    argsConfig.common.etlPart,
    Some(extractor),
    Some(transformer),
    Some(() => Seq(
      TableRegistry.Vacancies,
      TableRegistry.Locations,
      TableRegistry.Fields
    )),
    Some(() => fileConfig.common.updateLimit)
  )

  spark.stop()
}
