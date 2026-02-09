package org.example.habrcareer

import org.example.config.TableConfig.TableRegistry
import org.example.core.implement.HDFS.HDFSService
import org.example.core.implement.Network.{STTPBackendContext, STTPBackends, STTPService}
import org.example.core.implement.Postgres.PostgresService
import org.example.core.{ETLCycle, SparkApp}
import org.example.habrcareer.config.{HabrArgsLoader, HabrFileLoader}
import org.example.habrcareer.implement.{HabrExtractor, HabrTransformer}

object HabrCareerMain extends App {

  private val argsConfig = new HabrArgsLoader(args)
  private val fileConfig = new HabrFileLoader(argsConfig.common.confFile, argsConfig.common.saveFolder)

  private val spark = SparkApp.defineSession(fileConfig.structures.sparkConf, argsConfig.common.etlPart)

  private val ec = new ETLCycle(
    spark,
    new PostgresService(fileConfig.structures.dbConf),
    new HDFSService(fileConfig.structures.fsConf),
    new STTPService(fileConfig.structures.netConf, () => STTPBackendContext.getBackend(STTPBackends.DEFAULT))
  )

  private val extractor = new HabrExtractor(
    fileConfig.common.apiBaseUrl,
    fileConfig.vacsPageLimit,
    fileConfig.vacsPerPage,
    fileConfig.structures.netConf.requestsPS,
    fileConfig.common.rawPartitions
  )

  private val transformer = new HabrTransformer(
    fileConfig.common.transformPartitions
  )

  ec.run(
    argsConfig.common.etlPart,
    extractor = Some(extractor),
    transformer = Some(transformer),
    loader = Some(() => Seq(
      TableRegistry.Vacancies,
      TableRegistry.Fields,
      TableRegistry.Skills,
      TableRegistry.Locations
    )),
    updater = Some(() => 10_000)
  )

  spark.stop()
}