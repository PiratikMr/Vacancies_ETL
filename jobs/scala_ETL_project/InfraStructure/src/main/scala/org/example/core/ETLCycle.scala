package org.example.core

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.config.FolderName.FolderName
import org.example.config.TableConfig._
import org.example.core.Interfaces.ETL.{Extractor, Loader, Transformer, Updater}
import org.example.core.Interfaces.Services.{DataBaseService, StorageService, WebService}
import org.example.core.objects.ETLParts
import org.example.core.objects.ETLParts._

class ETLCycle(
                      spark: SparkSession,
                      dbService: DataBaseService,
                      fsService: StorageService,
                      webService: WebService
                    ) extends StrictLogging {

  private def extract(extractor: Extractor, writeTo: FolderName): Unit = {
    val rawDS = extractor.extract(spark, webService)
    fsService.writeText(rawDS, writeTo)
  }

  private def transform(transformer: Transformer, readFrom: FolderName): Unit = {
    val rawDS: Dataset[String] = fsService.readText(spark, readFrom)
    val rawDF: DataFrame = transformer.toRows(spark, rawDS)
    val DFs = transformer.transform(spark, rawDF)

    DFs.foreach { case(folder, df) =>
      fsService.write(df, folder)
    }
  }

  private def load(loader: Loader): Unit = {

    val data = loader.tablesToLoad().map(ld =>
      ld -> fsService.read(spark, ld.folder)
    )

    data.foreach { case(ld, df) =>
      val updateCols = ld.config.updateStrategy match {
        case DoNothing => None
        case ExplicitUpdates(cols) => Some(cols)
        case UpdateAllExceptKeys =>
          Some(df.columns.filterNot(ld.config.conflicts.contains).toSeq)
      }
      dbService.save(df, ld.folder, ld.config.conflicts, updateCols)
    }
  }

  private def update(extractor: Extractor, updater: Updater): Unit = {
    val activeIds: DataFrame = dbService
      .getActiveVacancies(spark, updater.updateLimit())

    val unActiveIds: DataFrame = extractor.filterUnActiveVacancies(spark, activeIds, webService)

    dbService.updateActiveVacancies(unActiveIds)
  }


  def run(
           etlPart: String,
           extractor: Option[Extractor] = None,
           transformer: Option[Transformer] = None,
           loader: Option[Loader] = None,
           updater: Option[Updater] = None,
           rawFolder: FolderName = FolderName.RawVacancies
         ): Unit = {

    logger.info(s"Launch ETL process: $etlPart")

    ETLParts.parseString(etlPart) match {
      case EXTRACT =>
        extractor match {
          case Some(ext) => extract(ext, rawFolder)
          case None => throw new UnsupportedOperationException("Module does not support EXTRACT operation.")
        }
      case TRANSFORM =>
        transformer match {
          case Some(tr) => transform(tr, rawFolder)
          case None => throw new UnsupportedOperationException("Module does not support TRANSFORM operation.")
        }
      case LOAD =>
        loader match {
          case Some(ld) => load(ld)
          case None => throw new UnsupportedOperationException("Module does not support LOAD operation.")
        }
      case UPDATE =>
        (extractor, updater) match {
          case (Some(ext), Some(upd)) => update(ext, upd)
          case _ => throw new UnsupportedOperationException("Module does not support UPDATE operation.")
        }
      case UNRECOGNIZED =>
        throw new UnsupportedOperationException(s"Unknown ETL part: $etlPart")
    }
  }
}
