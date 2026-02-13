package org.example.core.normalization.impl.strategy

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.collect_list

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
