package org.example.currency.implement

import org.example.{TestObjects, TestUtils}
import org.example.config.FolderName.{FolderName, Stage}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class CurrencyTransformerTest extends AnyFlatSpec {

  private val spark = TestObjects.spark
  import spark.implicits._


  private val expected = Seq(
    ("RUB", 1.0),
    ("AED", 0.04704),
    ("AFN", 0.8409),
    ("ALL", 1.0444),
    ("AMD", 4.8734)
  ).toDF("id", "rate")

  private lazy val rawDF = spark.read
    .json(Seq(TestUtils.readFile("rawData.json").replace("\n", "")).toDS)
    .as("data")


  "transform" should "map key value format to array" in {

    val actual = CurrencyTransformer.transform(spark, rawDF)(FolderName(Stage, "Currency"))

    actual.count() shouldBe expected.count()

    val actualRows = actual.collect().sortBy(_.getDouble(1)).toList
    val expectedRows = expected.collect().sortBy(_.getDouble(1)).toList

    actualRows shouldBe expectedRows
  }
}
