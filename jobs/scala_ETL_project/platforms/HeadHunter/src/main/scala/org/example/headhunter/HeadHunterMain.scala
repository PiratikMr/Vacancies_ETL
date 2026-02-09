package org.example.headhunter

import org.apache.spark.sql.functions.col
import org.example.core.adapter.database.impl.postgres.PostgresAdapter
import org.example.core.adapter.storage.impl.hdfs.HDFSAdapter
import org.example.core.adapter.web.impl.sttp.{STTPAdapter, STTPBackendContext, STTPBackends}
import org.example.core.etl.ETLUService
import org.example.core.util.SparkApp
import org.example.headhunter.config.{FolderNames, HHArgsLoader, HHFileLoader}
import org.example.headhunter.implement.{HHExtractor, HHTransformer}

object HeadHunterMain extends App {

  private val argsConfig = new HHArgsLoader(args)
  private val fileConfig = new HHFileLoader(argsConfig.common.confFile, argsConfig.common.saveFolder)

  private val spark = SparkApp.defineSession(fileConfig.structures.sparkConf, argsConfig.common.etlPart)

  private val dbService = new PostgresAdapter(fileConfig.structures.dbConf)

  import spark.implicits._

  private val hdfsAdapter = new HDFSAdapter(fileConfig.structures.fsConf)
  private val profRolesIds = hdfsAdapter.read(spark, FolderNames.roles, withDate = false)
    .where(col("field_id").isin(fileConfig.fieldIDs: _*))
    .select("role_id")
    .map(row => row.getLong(0))


  private val ec = new ETLUService(
    spark,
    dbService,
    hdfsAdapter,
    new STTPAdapter(fileConfig.structures.netConf, () => STTPBackendContext.getBackend(STTPBackends.DEFAULT))
  )

  private val extractor = new HHExtractor(
    profRolesIds,
    fileConfig.common.apiBaseUrl,
    argsConfig.dateFrom,
    fileConfig.pageLimit,
    fileConfig.vacsPerPage,
    fileConfig.structures.netConf.requestsPS,
    fileConfig.common.rawPartitions
  )

  val areas = hdfsAdapter.read(spark, FolderNames.areas, withDate = false)
    .withColumnRenamed("id", "area_id")

  ec.run(
    argsConfig.common.etlPart,
    extractor = Some(extractor),
    transformer = Some(new HHTransformer(areas, dbService, fileConfig.structures.fuzzyMatcherConf)),
    updater = Some(() => fileConfig.common.updateLimit)
  )

  spark.stop()
}
