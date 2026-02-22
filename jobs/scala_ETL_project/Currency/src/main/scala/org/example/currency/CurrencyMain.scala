package org.example.currency

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, MapType, StringType}
import org.example.core.adapter.database.impl.postgres.PostgresAdapter
import org.example.core.adapter.web.impl.sttp.STTPAdapter
import org.example.core.config.model.structures.SparkConf
import org.example.core.config.schema.SchemaRegistry
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

  private val entity = SchemaRegistry.DataBase.Entities.Currency


  private val body = sttpAdapter.readBodyOrThrow(
    s"${fileConfig.common.apiBaseUrl}/${fileConfig.apiKey}/latest/RUB"
  )

  import spark.implicits._

  val ds = Seq(body).toDS()

  private val rawDf = spark.read.option("multiLine", "true").json(ds).as("data")

  private val transformedDf = rawDf.select(explode(
    from_json(
      to_json(col("data.conversion_rates")),
      MapType(StringType, DoubleType)
    )
  ).as(Seq(entity.dimTable.name.name, entity.rate.name)))

  private val savedDimDf = dbAdapter.saveWithReturn(
    spark = spark,
    df = transformedDf,
    targetTable = entity.dimTable.tableName,
    returns = Seq(entity.dimTable.entityId.name, entity.dimTable.name.name),
    conflicts = Seq(entity.dimTable.name.name),
    updates = Some(Seq(entity.rate.name))
  ).cache()

  private val fuzzySettings = fileConfig.structures.fuzzyMatcherConf.get(NormalizersEnum.CURRENCY)
  private val similarityStrategy = new DefaultSimilarityStrategy(fuzzySettings)

  private val mappingDf = savedDimDf
    .withColumn(entity.mappingDimTable.mappedValue.name, similarityStrategy.normalize(col(entity.dimTable.name.name)))
    .withColumn(entity.mappingDimTable.isCanonical.name, lit(true))
    .select(
      entity.mappingDimTable.entityId.name,
      entity.mappingDimTable.mappedValue.name,
      entity.mappingDimTable.isCanonical.name
    )

  dbAdapter.save(
    df = mappingDf,
    targetTable = entity.mappingDimTable.tableName,
    conflicts = Seq(entity.mappingDimTable.mappedValue.name, entity.mappingDimTable.entityId.name),
    updates = Some(Seq(entity.mappingDimTable.isCanonical.name))
  )

  savedDimDf.unpersist(blocking = false)
}