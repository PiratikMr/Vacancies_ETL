package org.example.core.normalization.impl.strategy

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}

abstract class RawDataExtractor {
  def extract(df: DataFrame, entityIdCol: String, valueCol: String): DataFrame
}

object Extractors {

  object SimpleExtractor extends RawDataExtractor {
    def extract(df: DataFrame, entityIdCol: String, valueCol: String): DataFrame = {
      df.select(entityIdCol, valueCol)
    }
  }

  object ArrayExtractor extends RawDataExtractor {
    def extract(df: DataFrame, entityIdCol: String, valueCol: String): DataFrame = {
      df.select(
        col(entityIdCol),
        explode(col(valueCol)).as(valueCol)
      )
    }
  }

}
