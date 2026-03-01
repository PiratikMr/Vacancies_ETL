package org.example.currency

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, MapType, StringType}
import org.example.core.adapter.database.impl.postgres.PostgresAdapter
import org.example.core.adapter.web.impl.sttp.STTPAdapter
import org.example.core.config.database.{DimCurrencyDef, MappingCurrencyDef}
import org.example.core.config.model.structures.SparkConf
import org.example.core.normalization.engine.similarity.impl.DefaultSimilarityStrategy
import org.example.core.normalization.model.NormalizersEnum
import org.example.core.util.SparkJob
import org.example.currency.config.{CurrencyArgsLoader, CurrencyFileLoader}

object CurrencyMain extends App with SparkJob {

  private val argsConfig = new CurrencyArgsLoader(args)
  private val fileConfig = new CurrencyFileLoader(argsConfig.common.confFile)

  override def sparkConf: SparkConf = fileConfig.structures.sparkConf

  override def sparkName: String = s"Currency"


  private val dbAdapter = new PostgresAdapter(fileConfig.structures.dbConf)
  private val sttpAdapter = STTPAdapter(fileConfig.structures.netConf)

  private val body = sttpAdapter.readBodyOrThrow(
    s"${fileConfig.common.apiBaseUrl}/${fileConfig.apiKey}/latest/RUB"
  )

  import spark.implicits._

  private val rawDf = spark.read.option("multiLine", "true").json(Seq(body).toDS()).as("data")


  private val dimDef = DimCurrencyDef

  private val transformedDf = rawDf.select(explode(
    from_json(
      to_json(col("data.conversion_rates")),
      MapType(StringType, DoubleType)
    )
  ).as(Seq(dimDef.entityName, dimDef.rate)))

  private val savedDimDf = dbAdapter.saveWithReturn(
    spark = spark,
    df = transformedDf,
    targetTable = dimDef.meta.tableName,
    returns = Seq(dimDef.entityId, dimDef.entityName),
    conflicts = dimDef.meta.conflictKeys,
    updates = Some(Seq(dimDef.rate))
  ).cache()

  private val fuzzySettings = fileConfig.structures.fuzzyMatcherConf.get(NormalizersEnum.CURRENCY)
  private val similarityStrategy = new DefaultSimilarityStrategy(fuzzySettings)

  private val mappingDef = MappingCurrencyDef

  private val mappingDf = savedDimDf
    .withColumn(mappingDef.mappedValue, similarityStrategy.normalize(col(dimDef.entityName)))
    .withColumn(mappingDef.isCanonical, lit(true))
    .select(
      mappingDef.entityId,
      mappingDef.mappedValue,
      mappingDef.isCanonical
    )

  dbAdapter.save(
    df = mappingDf,
    targetTable = mappingDef.meta.tableName,
    conflicts = mappingDef.meta.conflictKeys
  )

  savedDimDf.unpersist(blocking = false)
}