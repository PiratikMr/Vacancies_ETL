package org.example.core.normalization.impl

import org.apache.spark.sql.DataFrame
import org.example.core.normalization.api.Normalizer
import org.example.core.normalization.impl.strategy.{RawDataExtractor, ResultAggregator}
import org.example.core.normalization.model.NormalizeServiceResult
import org.example.core.normalization.service.NormalizeService

class GenericNormalizer(normalizeService: NormalizeService,
                        extractor: RawDataExtractor,
                        aggregator: ResultAggregator,
                        entityIdCol: String,
                        valCol: String
                       ) extends Normalizer {

  override def extractTags(df: DataFrame, valueCol: String): NormalizeServiceResult = {

    val normalized = normalizeService.extractTags(
      candidates = df.select(entityIdCol, valueCol),
      entityIdCol = entityIdCol,
      valueCol = valueCol
    )

    val finalRes = aggregator.aggregate(normalized.mappedDf, entityIdCol, normalized.mappedIdCol)

    NormalizeServiceResult(finalRes, normalized.mappedIdCol)
  }


  override def matchExactData(df: DataFrame): NormalizeServiceResult =
    normalize(df, withCreate = false)


  override def normalize(df: DataFrame): NormalizeServiceResult =
    normalize(df, withCreate = true)


  private def normalize(df: DataFrame,
                        withCreate: Boolean): NormalizeServiceResult = {

    val rawData = extractor.extract(df, entityIdCol, valCol)

    val normalized = normalizeService.mapSimple(
      candidates = rawData,
      entityIdCol = entityIdCol,
      valueCol = valCol,
      parentIdCol = None,
      withCreate = withCreate
    )

    val finalRes = aggregator.aggregate(normalized.mappedDf, entityIdCol, normalized.mappedIdCol)

    NormalizeServiceResult(finalRes, normalized.mappedIdCol)
  }
}
