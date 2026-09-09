package org.example.core.normalization.api

import org.apache.spark.sql.Dataset
import org.example.core.etl.model.Vacancy
import org.example.core.normalization.model.NormalizationOutput

trait NormalizationCommand {
  def execute(ds: Dataset[Vacancy]): NormalizationOutput
}
