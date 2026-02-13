package org.example.core.normalization.service

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.model.structures.FuzzyMatcherConf
import org.example.core.config.schema.SchemaRegistry
import org.example.core.normalization.api.{NormalizationTask, Normalizer}
import org.example.core.normalization.factory.NormalizerFactory
import org.example.core.objects.NormalizersEnum._

class NormalizationOrchestrator(spark: SparkSession,
                                dbAdapter: DataBaseAdapter,
                                conf: FuzzyMatcherConf) {

  def normalize(tasks: Seq[NormalizationTask], df: DataFrame): DataFrame = {

    tasks.foldLeft(df)((res, task) => {
      val normalizer = getNormalizer(task.nType)

      val mappedResult = task match {
        case NormalizationTask.Standard(_) => normalizer.normalize(df)
        case NormalizationTask.Exact(_) => normalizer.matchExactData(df)
        case NormalizationTask.ExtractTags(_, sourceCol) => normalizer.extractTags(df, sourceCol)
      }

      val mappedDf = task.nType match {
        case LANGUAGES => mappedResult.mappedDf
          .withColumnRenamed(s"${mappedResult.mappedIdCol}_1", LANGUAGES_LEVEL.mappedIdCol)
          .withColumnRenamed(s"${mappedResult.mappedIdCol}_2", LANGUAGES.mappedIdCol)

        case _ => mappedResult.mappedDf.withColumnRenamed(mappedResult.mappedIdCol, task.nType.mappedIdCol)
      }


      res.join(
          mappedDf,
          Seq(SchemaRegistry.Internal.RawVacancy.externalId.name),
          "left"
        )
        .localCheckpoint()
    })
  }

  private def getNormalizer(nType: NormalizerType): Normalizer = {

    nType match {
      case LANGUAGES => NormalizerFactory.getLanguageNormalizer(spark, dbAdapter, conf.get(LANGUAGES_LEVEL), conf.get(LANGUAGES))

      case v: GroupNonHierarchical => NormalizerFactory.getNonHierarchicalNormalizer(spark, dbAdapter, conf.get(nType), v)

      case LOCATIONS => NormalizerFactory.getLocationsNormalizer(spark, dbAdapter, conf.get(LOCATIONS), conf.get(COUNTRY))
    }
  }

}
