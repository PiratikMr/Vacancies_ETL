package org.example.core.normalization.impl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.example.core.config.schema.DataBaseOneToManyEntity
import org.example.core.config.schema.SchemaRegistry.DataBase.Entities
import org.example.core.config.schema.SchemaRegistry.Internal.NormalizedVacancy
import org.example.core.normalization.api.Normalizer
import org.example.core.normalization.config.{DimTableConf, MappingDimTableConf}
import org.example.core.normalization.model.NormalizeServiceResult
import org.example.core.normalization.service.NormalizeService

class LanguageNormalizer(spark: SparkSession,
                         dbAdapter: DataBaseAdapter,
                         languageSettings: FuzzyMatchSettings,
                         levelSettings: FuzzyMatchSettings,
                         entityIdCol: String,
                         arrayCol: String,
                         languageCol: String,
                         levelCol: String) extends Normalizer {

  override def extractTags(data: DataFrame, valueCol: String): NormalizeServiceResult = ???

  override def matchExactData(data: DataFrame): NormalizeServiceResult = ???

  override def normalize(data: DataFrame): NormalizeServiceResult = {

    val rawData = data.select(col(entityIdCol), explode(col(arrayCol)).as(arrayCol))
      .withColumn(uniqueId, monotonically_increasing_id())
      .cache()


    def normalizeData(nameCol: String, service: NormalizeService): (DataFrame, String) = {
      val data = rawData.select(col(uniqueId), col(arrayCol).getField(nameCol).as(nameCol))
      val result = service.mapSimple(
        candidates = data,
        entityIdCol = uniqueId,
        valueCol = nameCol,
        parentIdCol = None,
        withCreate = true
      )
      (result.mappedDf.localCheckpoint(), result.mappedIdCol)
    }

    val (nLevels, levelMappedIdCol) = normalizeData(levelCol, levelNormalizeService)
    val (nLanguages, languageMappedIdCol) = normalizeData(languageCol, languageNormalizeService)


    val mappedIdCol1 = "mapped_id_1"
    val mappedIdCol2 = "mapped_id_2"

    val finalRes = rawData.select(entityIdCol, uniqueId)
      .join(nLevels.withColumnRenamed(levelMappedIdCol, mappedIdCol1), Seq(uniqueId), "left")
      .join(nLanguages.withColumnRenamed(languageMappedIdCol, mappedIdCol2), Seq(uniqueId), "left")
      .groupBy(entityIdCol)
      .agg(
        collect_list(
          struct(
            col(mappedIdCol1).as(NormalizedVacancy.languageLevel.name),
            col(mappedIdCol2).as(NormalizedVacancy.languageLanguage.name)
          )
        ).as(NormalizedVacancy.languages.name)
      )

    NormalizeServiceResult(finalRes, "mapped_ids")
  }

  private val uniqueId = "unique_id_896758"


  private val languageNormalizeService = createNormalizer(Entities.Languages)

  private val levelNormalizeService = createNormalizer(Entities.LanguageLevels)

  private def createNormalizer(entity: DataBaseOneToManyEntity): NormalizeService = {

    val dt = entity.dimTable
    val mdt = entity.mappingDimTable

    val settings = entity match {
      case Entities.Languages => languageSettings
      case Entities.LanguageLevels => levelSettings
    }

    new NormalizeService(
      spark = spark,
      dbAdapter = dbAdapter,
      settings = settings,
      dt = DimTableConf(dt.tableName, dt.entityId.name, dt.name.name, None),
      mdt = MappingDimTableConf(mdt.tableName, mdt.entityId.name, mdt.mappedValue.name, mdt.isCanonical.name)
    )
  }
}