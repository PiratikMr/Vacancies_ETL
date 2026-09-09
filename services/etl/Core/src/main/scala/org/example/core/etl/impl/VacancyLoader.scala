package org.example.core.etl.impl

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.database._
import org.example.core.etl.Loader
import org.example.core.etl.model.NormalizationResult
import org.example.core.etl.model.VacancyColumns._
import org.example.core.normalization.model.{MatchLogPart, NormalizationColumns}
import org.example.core.util.mapper.VacancyDBMapper

import scala.collection.parallel.CollectionConverters._

class VacancyLoader(dbAdapter: DataBaseAdapter) extends Loader with LazyLogging {

  private val factDef = FactVacancyDef

  override def load(spark: SparkSession, result: NormalizationResult): Unit = {

    val ds = result.vacancies

    val factVacancy = VacancyDBMapper.toFactVacancyTable(spark, ds)
      .toDF()
      .drop(factDef.vacancyId)

    val returnIds = dbAdapter.saveWithReturn(
      spark, factVacancy, factDef.meta.tableName,
      returns = Seq(factDef.vacancyId, factDef.externalId),
      conflicts = factDef.meta.conflictKeys,
      updates = Some(Seq(factDef.closedAt, factDef.updatedAt))
    )

    val dfWithId = ds.toDF().join(
      returnIds,
      ds(EXTERNAL_ID) === returnIds(factDef.externalId)
    ).cache()


    val simpleBridges = Seq(
      (BridgeVacancyEmploymentDef, EMPLOYMENT_IDS),
      (BridgeVacancyFieldDef, FIELD_IDS),
      (BridgeVacancyGradeDef, GRADE_IDS),
      (BridgeVacancyLocationDef, LOCATIONS),
      (BridgeVacancyScheduleDef, SCHEDULE_IDS),
      (BridgeVacancySkillDef, SKILL_IDS)
    )

    simpleBridges.par.foreach { case (bridgeDef, arrayColName) =>
      loadBridgeHelper(dfWithId, bridgeDef, arrayColName)
    }


    val languagesToWrite = dfWithId
      .withColumn("lang", explode(col(LANGUAGES)))
      .select(
        col(factDef.vacancyId),
        col(s"lang.$LANGUAGE_ID").as(BridgeVacancyLanguageDef.entityId),
        col(s"lang.$LEVEL_ID").as(BridgeVacancyLanguageDef.languageLevelId)
      )
      .distinct()
      .cache()

    if (!languagesToWrite.isEmpty) {
      dbAdapter.save(
        languagesToWrite,
        BridgeVacancyLanguageDef.meta.tableName,
        BridgeVacancyLanguageDef.meta.conflictKeys
      )
    }

    result.matchLogs.par.foreach(loadMatchLogHelper(_, returnIds))

    languagesToWrite.unpersist(blocking = false)
    dfWithId.unpersist(blocking = false)
  }


  private def loadBridgeHelper(df: DataFrame, bridge: BridgeDef, arrayColName: String): Unit = {
    val toWrite = df
      .withColumn(bridge.entityId, explode(col(arrayColName)))
      .select(
        col(factDef.vacancyId),
        col(bridge.entityId)
      )
      .distinct()
      .cache()

    dbAdapter.save(toWrite, bridge.meta.tableName, bridge.meta.conflictKeys)

    toWrite.unpersist(blocking = false)
  }


  private def loadMatchLogHelper(part: MatchLogPart, returnIds: DataFrame): Unit = {
    val logDef = part.logDef

    val toWrite = part.rows
      .join(returnIds, part.rows(EXTERNAL_ID) === returnIds(factDef.externalId))
      .select(
        col(factDef.vacancyId).as(logDef.vacancyId),
        col(NormalizationColumns.MAPPING_ID).as(logDef.mappingId),
        col(NormalizationColumns.RAW_VALUE).as(logDef.rawValue),
        col(NormalizationColumns.SCORE).as(logDef.score)
      )
      .distinct()
      .cache()

    dbAdapter.save(toWrite, logDef.meta.tableName, logDef.meta.conflictKeys)

    toWrite.unpersist(blocking = false)
  }

}
