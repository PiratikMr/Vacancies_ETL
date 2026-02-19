package org.example.core.etl

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.adapter.storage.StorageAdapter
import org.example.core.adapter.web.WebAdapter
import org.example.core.config.schema.SchemaRegistry
import org.example.core.etl.impl.VacancyLoader
import org.example.core.etl.model.ETLParts
import ETLParts.{Extract, TransformLoad, Update}
import org.example.core.util.SparkExtensions._

import scala.util.{Failure, Success}

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

  private def transform(transformer: Transformer, folderName: String): DataFrame = {
    val rawDS: Dataset[String] = storageAdapter.readText(spark, folderName)
    val rawDF: DataFrame = transformer.toRows(spark, rawDS)

    val transformed = transformer.transform(spark, rawDF)
      .smartSelect(SchemaRegistry.Internal.RawVacancy.schema)
      .dropDuplicates(SchemaRegistry.Internal.RawVacancy.externalId.name)
      .localCheckpoint()

    val normalized = transformer.normalize(spark, transformed)
      .smartSelect(SchemaRegistry.Internal.NormalizedVacancy.schema)
      .dropDuplicates(SchemaRegistry.Internal.NormalizedVacancy.externalId.name)
      .localCheckpoint()

    normalized
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
           extractor: Extractor,
           transformer: Transformer,
           loader: Option[Loader] = None,
           updater: Option[Updater] = None,
           folderName: String = "Vacancies"
         ): Unit = {

    ETLParts.parse(etlPart) match {
      case Success(Extract) =>
        extract(extractor, folderName)

      case Success(TransformLoad) =>
        val df = transform(transformer, folderName)
        loader.getOrElse(new VacancyLoader(dbAdapter)).load(spark, df)

      case Success(Update) =>
        updater match {
          case Some(upd) => update(extractor, upd)
          case None =>
            logger.error(s"Ошибка запуска ETL: Update не поддерживается")
        }

      case Failure(e) =>

        logger.error(s"Ошибка запуска ETL: ${e.getMessage}")

        throw e
    }
  }
}