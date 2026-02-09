package org.example.core.normalization

import org.apache.spark.sql.DataFrame
import org.example.core.normalization.model.NormalizeServiceResult

trait Normalizer {

  /**
   * @param data [entityIdCol, valueCol]
   * @return [entityIdCol, mappedId]: DataFrame
   */
  def extractTags(data: DataFrame, valueCol: String): NormalizeServiceResult

  /**
   * @param data [idCol, valCol]: DataFrame
   * @return [entityId, mappedId]: DataFrame
   */
  def matchExactData(data: DataFrame): NormalizeServiceResult

  /**
   * @param data [idCol, valCol]: DataFrame
   * @return [entityId, array(mappedId)]: DataFrame
   */
  def normalize(data: DataFrame): NormalizeServiceResult
}