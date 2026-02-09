package org.example.core.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class SchemaBridge(toInternalMapping: Seq[(String, String)]) {

  private val toExternalMapping = toInternalMapping.map(_.swap)


  def toInternal(df: DataFrame): DataFrame = {
    applyMapping(df, toInternalMapping)
  }

  def toExternal(df: DataFrame): DataFrame = {
    applyMapping(df, toExternalMapping)
  }


  private def applyMapping(df: DataFrame, m: Seq[(String, String)]): DataFrame = {

    val existingCols = df.columns.toSet

    val pairs = m.filter { case (src, _) => existingCols.contains(src) }

    val cols = pairs.map { case (src, dest) =>
      col(src).as(dest)
    }

    df.select(cols: _*)
  }
}
