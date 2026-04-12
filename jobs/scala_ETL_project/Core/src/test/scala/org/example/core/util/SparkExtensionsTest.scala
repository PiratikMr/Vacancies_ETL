package org.example.core.util

import org.apache.spark.sql.types._
import org.example.SparkEnv
import org.example.core.util.SparkExtensions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkExtensionsTest extends AnyFlatSpec with Matchers with SparkEnv {

  import spark.implicits._

  private def targetSchema(fields: (String, DataType, Boolean)*): StructType =
    StructType(fields.map { case (name, dt, nullable) => StructField(name, dt, nullable) })

  "smartSelect" should "select only columns specified in targetSchema" in {
    val df = Seq((1, "Alice", "unused")).toDF("id", "name", "extra")
    val schema = targetSchema(("id", IntegerType, false), ("name", StringType, true))

    val result = df.smartSelect(schema)

    result.columns.toSeq shouldBe Seq("id", "name")
  }

  it should "add null column for optional missing fields" in {
    val df = Seq((1, "Alice")).toDF("id", "name")
    val schema = targetSchema(
      ("id", IntegerType, false),
      ("name", StringType, true),
      ("age", LongType, true)
    )

    val result = df.smartSelect(schema)
    val row = result.collect().head

    result.columns should contain("age")
    assert(row.get(result.schema.fieldIndex("age")) == null)
  }

  it should "throw IllegalArgumentException for missing non-nullable column" in {
    val df = Seq((1, "Alice")).toDF("id", "name")
    val schema = targetSchema(
      ("id", IntegerType, false),
      ("missing_required", StringType, false)
    )

    intercept[IllegalArgumentException] {
      df.smartSelect(schema)
    }
  }

  it should "preserve column order from targetSchema" in {
    val df = Seq((1, "Alice", 100L)).toDF("id", "name", "score")
    val schema = targetSchema(
      ("score", LongType, true),
      ("name", StringType, true),
      ("id", IntegerType, false)
    )

    val result = df.smartSelect(schema)

    result.columns.toSeq shouldBe Seq("score", "name", "id")
  }
}
