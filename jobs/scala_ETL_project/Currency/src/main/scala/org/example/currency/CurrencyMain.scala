package org.example.currency

import org.example.config.FolderName.{FolderName, Raw, Stage}
import org.example.config.TableConfig.{TableConfig, UpdateAllExceptKeys}
import org.example.core.implement.HDFS.HDFSService
import org.example.core.implement.Network.{STTPBackendContext, STTPBackends, STTPService}
import org.example.core.implement.Postgres.PostgresService
import org.example.core.objects.LoadDefinition
import org.example.core.{ETLCycle, SparkApp}
import org.example.currency.config.{CurrencyArgsLoader, CurrencyFileLoader}
import org.example.currency.implement.{CurrencyExtractor, CurrencyTransformer}

object CurrencyMain extends App {

  private val argsConfig = new CurrencyArgsLoader(args)
  private val fileConfig = new CurrencyFileLoader(argsConfig.common.confFile, argsConfig.common.saveFolder)

  private val spark = SparkApp.defineSession(fileConfig.structures.sparkConf, argsConfig.common.etlPart)

  private val ec = new ETLCycle(
    spark,
    new PostgresService(fileConfig.structures.dbConf),
    new HDFSService(fileConfig.structures.fsConf),
    new STTPService(fileConfig.structures.netConf, () => STTPBackendContext.getBackend(STTPBackends.DEFAULT))
  )

  ec.run(
    argsConfig.common.etlPart,
    extractor = Some(new CurrencyExtractor(fileConfig.common.apiBaseUrl, fileConfig.apiKey)),
    transformer = Some(CurrencyTransformer),
    loader = Some(() => Seq(
      LoadDefinition(
        FolderName(Stage, "Currency"),
        TableConfig(Seq("id"), UpdateAllExceptKeys)
      )
    )),
    rawFolder = FolderName(Raw, "Currency")
  )

  spark.stop()
}
