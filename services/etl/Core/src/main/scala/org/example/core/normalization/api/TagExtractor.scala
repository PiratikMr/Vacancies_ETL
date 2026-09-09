package org.example.core.normalization.api

import org.apache.spark.sql.Dataset
import org.example.core.etl.model.Vacancy
import org.example.core.normalization.model.NormalizationOutput

trait TagExtractor {
  def extractTags(ds: Dataset[Vacancy], sourceCol: String): NormalizationOutput
}
