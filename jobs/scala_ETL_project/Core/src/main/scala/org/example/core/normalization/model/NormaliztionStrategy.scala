package org.example.core.normalization.model

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_list, explode}

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



trait ResultAggregator {
  def aggregate(df: DataFrame, entityIdCol: String, mappedIdCol: String): DataFrame
}

object Aggregators {

  object NoAggregation extends ResultAggregator {
      override def aggregate(df: DataFrame, entityIdCol: String, mappedIdCol: String): DataFrame = {
        df.select(entityIdCol, mappedIdCol)
      }
  }

  object ArrayAggregator extends ResultAggregator {
    override def aggregate(df: DataFrame, entityIdCol: String, mappedIdCol: String): DataFrame = {
      df.groupBy(entityIdCol)
        .agg(collect_list(mappedIdCol).as(mappedIdCol))
    }
  }

}