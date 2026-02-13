package org.example.core.etl

import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.adapter.storage.StorageAdapter
import org.example.core.adapter.web.WebAdapter
import org.example.core.config.schema.SchemaRegistry.DataBase.Entities._
import org.example.core.config.schema.SchemaRegistry.DataBase.FactVacancy
import org.example.core.config.schema.SchemaRegistry.Internal.NormalizedVacancy
import org.example.core.config.schema.{DataBaseBridgeTable, SchemaRegistry}
import org.example.core.objects.ETLParts
import org.example.core.objects.ETLParts._
import com.typesafe.scalalogging.LazyLogging
import org.example.core.util.SparkExtensions._

class ETLUService(
                   spark: SparkSession,
                   dbAdapter: DataBaseAdapter,
                   storageAdapter: StorageAdapter,
                   webAdapter: WebAdapter
                 ) extends LazyLogging {

  private def extract(extractor: Extractor, folderName: String): Unit = {
    val rawDS = extractor.extract(spark, webAdapter)
    storageAdapter.writeText(rawDS, folderName)
  }

  private def transform(transformer: Transformer, folderName: String): Unit = {
    val rawDS: Dataset[String] = storageAdapter.readText(spark, folderName)
    val rawDF: DataFrame = transformer.toRows(spark, rawDS)

    val transformed = transformer.transform(spark, rawDF)
      .smartSelect(SchemaRegistry.Internal.RawVacancy.schema)
      .dropDuplicates(SchemaRegistry.Internal.RawVacancy.externalId.name)
      .localCheckpoint()

    logger.debug(s"Трансформация завершена. Количество записей: ${transformed.count()}")

    transformed.show()

    val normalized = transformer.normalize(spark, transformed)
      .smartSelect(SchemaRegistry.Internal.NormalizedVacancy.schema)
      .dropDuplicates(SchemaRegistry.Internal.NormalizedVacancy.externalId.name)
      .localCheckpoint()

    logger.debug(s"Нормализация завершена. Количество записей: ${normalized.count()}")

    normalized.show()

    load(normalized)
  }

  private def load(df: DataFrame): Unit = {
    val mainTableSchema = SchemaRegistry.DataBase.FactVacancy

    val factVacancy = df.smartSelect(mainTableSchema.schema)
      .drop(mainTableSchema.vacancyId.name)

    logger.debug(s"Таблица фактов вакансий подготовлена. Количество: ${factVacancy.count()}")


    val returnIds = dbAdapter.saveWithReturn(
      spark, factVacancy, mainTableSchema.tableName,
      returns = Seq(mainTableSchema.vacancyId.name, mainTableSchema.externalId.name),
      conflicts = Seq(mainTableSchema.externalId.name, mainTableSchema.platformId.name)
    ).localCheckpoint()

    logger.debug(s"Получены ID сохраненных вакансий. Количество: ${returnIds.count()}")

    val dfWithId = df.join(
      returnIds,
      Seq(mainTableSchema.externalId.name)
    )

    logger.debug(s"Данные объединены с ID. Количество: ${dfWithId.count()}")

    dfWithId.show()


    Seq(Employments, Fields, Grades, Locations, Schedules, Skills)
      .foreach(entity => loadBridgeHelper(dfWithId, entity.bridge))

    val languagesToWrite = dfWithId
      .withColumn("lang", explode(col(NormalizedVacancy.languages.name)))
      .select(
        col(FactVacancy.vacancyId.name),
        col(s"lang.${NormalizedVacancy.languageLanguage.name}").as(NormalizedVacancy.languageLanguage.name),
        col(s"lang.${NormalizedVacancy.languageLevel.name}").as(NormalizedVacancy.languageLevel.name)
      )
      .distinct()

    logger.debug("Подготовка языков для сохранения...")
    languagesToWrite.show()

    if (!languagesToWrite.isEmpty)
      dbAdapter.save(languagesToWrite, Languages.bridge.tableName,
        Seq(FactVacancy.vacancyId.name, NormalizedVacancy.languageLevel.name, NormalizedVacancy.languageLanguage.name)
      )
  }

  private def loadBridgeHelper(df: DataFrame, bridge: DataBaseBridgeTable): Unit = {
    val toWrite = df
      .withColumn(bridge.entityId.name, explode(col(bridge.entityId.name)))
      .smartSelect(bridge.schema)
      .distinct()

    logger.debug(s"Связующая таблица ${bridge.tableName} подготовлена. Записей: ${toWrite.count()}")

    if (!toWrite.isEmpty)
      dbAdapter.save(toWrite, bridge.tableName, Seq(bridge.vacancyId.name, bridge.entityId.name))
  }

  private def update(extractor: Extractor, updater: Updater): Unit = {
    //    val activeIds: DataFrame = dbService
    //      .getActiveVacancies(spark, updater.updateLimit())
    //
    //    val unActiveIds: DataFrame = extractor.filterUnActiveVacancies(spark, activeIds, webService)
    //
    //    dbService.updateActiveVacancies(unActiveIds)
  }


  def run(
           etlPart: String,
           extractor: Option[Extractor] = None,
           transformer: Option[Transformer] = None,
           updater: Option[Updater] = None,
           folderName: String = "Vacancies"
         ): Unit = {

    ETLParts.parseString(etlPart) match {
      case EXTRACT =>
        extractor match {
          case Some(ext) => extract(ext, folderName)
          case None => unsupportedOperationException("EXTRACT")
        }
      case TRANSFORM_LOAD =>
        transformer match {
          case Some(tr) => transform(tr, folderName)
          case None => unsupportedOperationException("TRANSFORM_LOAD")
        }
      case UPDATE =>
        (extractor, updater) match {
          case (Some(ext), Some(upd)) => update(ext, upd)
          case _ => unsupportedOperationException("UPDATE")
        }
      case UNRECOGNIZED =>
        unsupportedOperationException(etlPart)
    }
  }

  private def unsupportedOperationException(part: String): Unit =
    throw new UnsupportedOperationException(s"Module does not support $part operation.")
}
