package org.example.core.etl.impl

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.schema.SchemaRegistry.DataBase.Entities._
import org.example.core.config.schema.SchemaRegistry.DataBase.FactVacancy
import org.example.core.config.schema.SchemaRegistry.Internal.NormalizedVacancy
import org.example.core.config.schema.{DataBaseBridgeTable, SchemaRegistry}
import org.example.core.etl.Loader
import org.example.core.util.SparkExtensions._

class VacancyLoader(dbAdapter: DataBaseAdapter) extends Loader with LazyLogging {

  override def load(spark: SparkSession, df: DataFrame): Unit = {

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

}