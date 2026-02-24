package org.example.core.etl.impl

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.database._
import org.example.core.etl.Loader
import org.example.core.etl.model.NormalizedVacancy
import org.example.core.etl.model.VacancyColumns._
import org.example.core.util.mapper.VacancyDBMapper

class VacancyLoader(dbAdapter: DataBaseAdapter) extends Loader with LazyLogging {

  override def load(spark: SparkSession, ds: Dataset[NormalizedVacancy]): Unit = {

    val factVacancy = VacancyDBMapper.toFactVacancyTable(spark, ds)
      .toDF()
      .drop(FactVacancyDef.vacancyId)

    val returnIds = dbAdapter.saveWithReturn(
      spark, factVacancy, FactVacancyDef.meta.tableName,
      returns = Seq(FactVacancyDef.vacancyId, FactVacancyDef.externalId),
      conflicts = FactVacancyDef.meta.conflictKeys
    )

    val dfWithId = ds.toDF().join(
      returnIds,
      ds(EXTERNAL_ID) === returnIds(FactVacancyDef.externalId)
    ).cache()


    val simpleBridges = Seq(
      (BridgeVacancyEmploymentDef, EMPLOYMENT_IDS),
      (BridgeVacancyFieldDef, FIELD_IDS),
      (BridgeVacancyGradeDef, GRADE_IDS),
      (BridgeVacancyLocationDef, LOCATIONS),
      (BridgeVacancyScheduleDef, SCHEDULE_IDS),
      (BridgeVacancySkillDef, SKILL_IDS)
    )

    simpleBridges.foreach { case (bridgeDef, arrayColName) =>
      loadBridgeHelper(dfWithId, bridgeDef, arrayColName)
    }


    val languagesToWrite = dfWithId
      .withColumn("lang", explode(col(LANGUAGES)))
      .select(
        col(FactVacancyDef.vacancyId),
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

    languagesToWrite.unpersist(blocking = false)
    dfWithId.unpersist(blocking = false)
  }


  private def loadBridgeHelper(df: DataFrame, bridge: BridgeDef, arrayColName: String): Unit = {
    val toWrite = df
      .withColumn(bridge.entityId, explode(col(arrayColName)))
      .select(
        col(FactVacancyDef.vacancyId),
        col(bridge.entityId)
      )
      .distinct()
      .cache()

    if (!toWrite.isEmpty)
      dbAdapter.save(toWrite, bridge.meta.tableName, bridge.meta.conflictKeys)

    toWrite.unpersist(blocking = false)
  }

}