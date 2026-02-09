package org.example.core.normalization.model

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.model.structures.FuzzyMatcherConf
import org.example.core.config.schema.SchemaRegistry
import org.example.core.normalization.Normalizer
import org.example.core.normalization.factory.NormalizerFactory
import org.example.core.objects.NormalizersEnum._

class NormalizerHelper(spark: SparkSession,
                       dbAdapter: DataBaseAdapter,
                       conf: FuzzyMatcherConf) {

  def normalize(normalizers: Seq[NormalizerType], df: DataFrame): DataFrame = {

    normalizers.foldLeft(df)((res, nType) => {
      val mappedResult = getNormalizer(nType)
        .normalize(df)

      val mappedDf = nType match {
        case LANGUAGES => mappedResult.mappedDf
          .withColumnRenamed(s"${mappedResult.mappedIdCol}_1", LANGUAGES_LEVEL.mappedIdCol)
          .withColumnRenamed(s"${mappedResult.mappedIdCol}_2", LANGUAGES.mappedIdCol)

        case _ => mappedResult.mappedDf.withColumnRenamed(mappedResult.mappedIdCol, nType.mappedIdCol)
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
