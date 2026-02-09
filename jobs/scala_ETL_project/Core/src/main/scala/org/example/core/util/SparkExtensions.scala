package org.example.core.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType

object SparkExtensions {

  implicit class DataFrameSmartOps(val df: DataFrame) extends AnyVal {
    def smartSelect(targetSchema: StructType): DataFrame = {

      val existingCols = df.schema.map(f => f.name -> f.dataType).toMap

      val cols = targetSchema.fields.map { f =>
        existingCols.get(f.name) match {
          case Some(actualType) =>
//            if (actualType != f.dataType) {
//              throw new IllegalArgumentException(
//                s"Column '${f.name}' has wrong type. Expected: ${f.dataType}, Found: $actualType"
//              )
//            }
            col(f.name)
          case None if !f.nullable =>
            throw new IllegalArgumentException(s"Missing required column: ${f.name}")

          case None =>
            lit(null).cast(f.dataType).as(f.name)
        }
      }

      df.select(cols: _*)
    }
  }
}


