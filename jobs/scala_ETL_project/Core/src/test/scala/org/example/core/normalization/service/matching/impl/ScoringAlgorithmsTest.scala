package org.example.core.normalization.service.matching.impl

import org.example.SparkEnv
import org.example.core.normalization.engine.similarity.impl.ScoringAlgorithms
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ScoringAlgorithmsTest extends AnyFlatSpec with Matchers with SparkEnv {

  import spark.implicits._

  // ==========================================
  // Levenshtein
  // ==========================================
  "levenshteinScore" should "calculate normalized similarity" in {
    val df = Seq(("abc", "abd")).toDF("v1", "v2")
    val score = df.select(ScoringAlgorithms.levenshteinScore($"v1", $"v2")).head().getDouble(0)
    score shouldBe 0.666 +- 0.001
  }

  it should "handle exact match" in {
    val df = Seq(("test", "test")).toDF("v1", "v2")
    val score = df.select(ScoringAlgorithms.levenshteinScore($"v1", $"v2")).head().getDouble(0)
    score shouldBe 1.0
  }

  // ==========================================
  // N-Grams
  // ==========================================
//  "nGramScore" should "calculate dice coefficient" in {
//
//    val df = Seq(("abc", "abd")).toDF("v1", "v2")
//    val score = df.select(ScoringAlgorithms.nGramScore($"v1", $"v2", n = 2)).head().getDouble(0)
//    score shouldBe 0.5
//  }
//
//  // ==========================================
//  // Number Factor
//  // ==========================================
//  "calculateNumberFactor" should "identify number match types" in {
//    def getFactor(v1: String, v2: String): Double = {
//      val df = Seq((v1, v2)).toDF("v1", "v2")
//      df.select(ScoringAlgorithms.calculateNumberFactor($"v1", $"v2")).head().getDouble(0)
//    }
//
//    getFactor("java 11", "java 11") shouldBe 1.0
//
//    getFactor("java 11", "java 17") shouldBe -1.0
//
//    getFactor("java", "java") shouldBe 0.0
//
//    getFactor("java 11", "java") shouldBe -1.0
//
//    getFactor("java 11 12", "java 11 13") shouldBe -0.35 +- 0.02
//  }
}