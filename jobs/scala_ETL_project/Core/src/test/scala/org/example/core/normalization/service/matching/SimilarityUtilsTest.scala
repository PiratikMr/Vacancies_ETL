package org.example.core.normalization.service.matching

import org.apache.spark.sql.functions.{array_join, array_sort}
import org.example.TestObjects
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class SimilarityUtilsTest extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

  private val spark = TestObjects.spark

  import spark.implicits._


  "normalize" should "correctly clean and stem input" in {

    val examples = Table(
      ("input", "expectedTokenParts"),

      ("Senior Java", Seq("_senior_", "_java_")),

      ("Разработчик программного обеспечения", Seq("_разработчик_", "_программн_", "_обеспечен_")),

      ("   JAVA!!! --- Developer... ", Seq("_java_", "_develop_")),

      ("Зелёный Чай", Seq("_зелены_", "_ча_")),

      ("C++ 17", Seq("_c++_", "_17_")),

      ("", Seq.empty[String]),
      ("!@$:^%", Seq.empty[String])
    )

    forAll(examples) { (input, expectedParts) =>
      val result = runNormalize(input)

      expectedParts.foreach { part =>
        result should contain(part)
      }

      result.length shouldBe expectedParts.length
    }
  }

  "normalize" should "handle nulls" in {
    val df = Seq(Option.empty[String]).toDF("text")
    val res = df.select(SimilarityUtils.normalize($"text"))
      .head().getAs[scala.collection.Seq[String]](0).toSeq

    res.length shouldBe 0
  }

  "calculateScore" should "score pairs according to expectations" in {
    val scenarios = Table(
      ("s1", "s2", "minScore", "maxScore"),

      ("Java Developer", "Java Developer", 0.99, 1.0),

      ("Senior Java", "Java Senior", 0.99, 1.0),

      ("Java Developer", "Java Developing", 0.9, 1.0),

      ("Java Developer", "Python Developer", 0.5, 0.7),

      ("Accounting Manager", "Driver", 0.0, 0.2),

      ("Hadoop Engineer", "Hadop Enginer", 0.7, 0.9)
    )

    forAll(scenarios) { (s1, s2, min, max) =>
      val score = runScore(s1, s2)
      println(score)
      score should be >= min
      score should be <= max
    }
  }

  "calculateScore" should "apply penalty when numbers differ" in {
    val s1 = "Java 11"
    val s2 = "Java 17"

    val scoreWithHighPenalty = runScore(s1, s2, 1.0)
    val scoreWithMediumPenalty = runScore(s1, s2, 0.5)
    val scoreWithoutPenalty = runScore(s1, s2, 0.0)

    scoreWithHighPenalty should be < scoreWithMediumPenalty
    scoreWithMediumPenalty should be < scoreWithoutPenalty
  }


  private def runNormalize(text: String): Seq[String] = {
    val df = Seq(text).toDF("text")
    val res = df.select(SimilarityUtils.normalize($"text").as("res"))
    res.head().getAs[scala.collection.Seq[String]](0).toSeq
  }

  private def runScore(v1: String, v2: String, penalty: Double = 0.1): Double = {
    val df = Seq((v1, v2)).toDF("v1", "v2")

    val n1 = array_join(array_sort(SimilarityUtils.normalize($"v1")), " ")
    val n2 = array_join(array_sort(SimilarityUtils.normalize($"v2")), " ")

    val res = df.select(
      SimilarityUtils.calculateScore(n1, n2, penalty).as("score")
    )
    res.head().getDouble(0)
  }

}
