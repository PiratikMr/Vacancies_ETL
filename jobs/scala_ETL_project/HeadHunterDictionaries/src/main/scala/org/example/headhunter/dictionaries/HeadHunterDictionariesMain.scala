package org.example.headhunter.dictionaries

import org.example.config.FolderName.{Dictionaries, FolderName, Raw}
import org.example.core.implement.HDFS.HDFSService
import org.example.core.implement.Network.{STTPBackendContext, STTPBackends, STTPService}
import org.example.core.implement.Postgres.PostgresService
import org.example.core.{ETLCycle, SparkApp}
import org.example.headhunter.dictionaries.config.{HHArgsLoader, HHFileLoader, HHFolderNames}
import org.example.headhunter.dictionaries.implement.{AreasTransformer, DictionariesTransformer, HHExtractor, RolesTransformer}

object HeadHunterDictionariesMain extends App {

  private val argsConfig = new HHArgsLoader(args)
  private val fileConfig = new HHFileLoader(argsConfig.common.confFile, argsConfig.common.saveFolder)

  private val spark = SparkApp.defineSession(fileConfig.structures.sparkConf, argsConfig.common.etlPart)

  private val ec = new ETLCycle(
    spark,
    new PostgresService(fileConfig.structures.dbConf),
    new HDFSService(fileConfig.structures.fsConf),
    new STTPService(fileConfig.structures.netConf, () => STTPBackendContext.getBackend(STTPBackends.DEFAULT))
  )

  private val url: String => String = endpoint => s"${fileConfig.common.apiBaseUrl}$endpoint"

  ec.run(
    argsConfig.common.etlPart,
    extractor = Some(new HHExtractor(url("/areas"))),
    transformer = Some(AreasTransformer),
    loader = Some(
      () => Seq(HHFolderNames.areas.loadDefinition)
    ),
    rawFolder = HHFolderNames.areas.raw
  )

  ec.run(
    argsConfig.common.etlPart,
    extractor = Some(new HHExtractor(url("/professional_roles"))),
    transformer = Some(RolesTransformer),
    loader = Some(
      () => Seq(HHFolderNames.roles.loadDefinition)
    ),
    rawFolder = HHFolderNames.roles.raw
  )

  ec.run(
    argsConfig.common.etlPart,
    extractor = Some(new HHExtractor(url("/dictionaries"))),
    transformer = Some(DictionariesTransformer),
    loader = Some(
      () => Seq(
        HHFolderNames.schedule.loadDefinition,
        HHFolderNames.employment.loadDefinition,
        HHFolderNames.experience.loadDefinition
      )
    ),
    rawFolder = FolderName(Raw, Dictionaries, "Dictionaries")
  )

  spark.stop()
}
