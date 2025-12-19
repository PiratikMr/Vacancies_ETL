package org.example.getmatch

import org.example.config.TableConfig.TableRegistry
import org.example.core.implement.HDFS.HDFSService
import org.example.core.implement.Network.{STTPBackendContext, STTPBackends, STTPService}
import org.example.core.implement.Postgres.PostgresService
import org.example.core.{ETLCycle, SparkApp}
import org.example.getmatch.config.{GetMatchArgsLoader, GetMatchFileLoader}
import org.example.getmatch.implement.{GetMatchExtractor, GetMatchTransformer}

object GetMatchMain extends App {

  private val argsConfig = new GetMatchArgsLoader(args)
  private val fileConfig = new GetMatchFileLoader(argsConfig.common.confFile, argsConfig.common.saveFolder)

  private val spark = SparkApp.defineSession(fileConfig.structures.sparkConf, argsConfig.common.etlPart)


  private val ec = new ETLCycle(
    spark,
    new PostgresService(fileConfig.structures.dbConf),
    new HDFSService(fileConfig.structures.fsConf),
    new STTPService(fileConfig.structures.netConf, () => STTPBackendContext.getBackend(STTPBackends.DEFAULT))
  )


  private val extractor = new GetMatchExtractor(
    fileConfig.getMatch,
    fileConfig.common.apiBaseUrl,
    fileConfig.structures.netConf.requestsPS,
    fileConfig.common.rawPartitions
  )

  private val transformer = new GetMatchTransformer(
    fileConfig.common.transformPartitions
  )

  ec.run(
    argsConfig.common.etlPart,
    extractor = Some(extractor),
    transformer = Some(transformer),
    loader = Some(() => Seq(
      TableRegistry.Vacancies,
      TableRegistry.Skills
    )),
    updater = Some(() => fileConfig.common.updateLimit)
  )

  spark.stop()
}
