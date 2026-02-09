package org.example.geekjob

import org.example.core.adapter.database.impl.postgres.PostgresAdapter
import org.example.core.adapter.storage.impl.hdfs.HDFSAdapter
import org.example.core.adapter.web.impl.sttp.{STTPAdapter, STTPBackendContext, STTPBackends}
import org.example.core.etl.ETLUService
import org.example.core.util.SparkApp
import org.example.geekjob.config.{GeekJobArgsLoader, GeekJobFileLoader}
import org.example.geekjob.implement.{GeekJobExtractor, GeekJobTransformer}

object GeekJobMain extends App {

  private val argsConfig = new GeekJobArgsLoader(args)
  private val fileConfig = new GeekJobFileLoader(argsConfig.common.confFile, argsConfig.common.saveFolder)

  private val spark = SparkApp.defineSession(fileConfig.structures.sparkConf, argsConfig.common.etlPart)


  private val dbAdapter = new PostgresAdapter(fileConfig.structures.dbConf)

  private val ec = new ETLUService(
    spark,
    dbAdapter,
    new HDFSAdapter(fileConfig.structures.fsConf),
    new STTPAdapter(fileConfig.structures.netConf, () => STTPBackendContext.getBackend(STTPBackends.UNSAFE))
  )

  private val extractor = new GeekJobExtractor(
    fileConfig.common.apiBaseUrl,
    fileConfig.pageLimit,
    fileConfig.structures.netConf.requestsPS,
    fileConfig.common.rawPartitions
  )

  private val transformer = new GeekJobTransformer(
    argsConfig.common.saveFolder,
    dbAdapter,
    fileConfig.structures.fuzzyMatcherConf,
    fileConfig.common.transformPartitions
  )

  ec.run(
    argsConfig.common.etlPart,
    extractor = Some(extractor),
    transformer = Some(transformer),
    updater = Some(() => fileConfig.common.updateLimit)
  )

  spark.stop()
}
