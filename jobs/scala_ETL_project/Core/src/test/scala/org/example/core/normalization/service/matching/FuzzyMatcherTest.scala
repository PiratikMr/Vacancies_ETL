package org.example.core.normalization.service.matching

import org.apache.spark.sql.functions.col
import org.example.SparkEnv
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FuzzyMatcherTest extends AnyFlatSpec with Matchers with SparkEnv {

  import spark.implicits._

  private val settings = FuzzyMatchSettings(score = 0.5, numberPenalty = 0.1)
  private val matcher = new FuzzyMatcher(spark, settings)

  private val cId = "vacancy_id"
  private val cVal = "value"
  private val cNorm = "norm_value"
  private val cPid = "parent_id"

  private val dId = "id"
  private val dVal = "norm_value"
  private val dPid = "parent_id"

  // --- Tests ---

  "FuzzyMatcher" should "match candidates against dictionary exactly" in {
    val dictionary = Seq(
      (10L, "java developer", 100)
    )
    val candidates = Seq(
      (1, "Java Developer", 100),   // Идеальное совпадение
      (2, "Python Developer", 100)  // Нет в словаре
    )

    val result = executeMatcher(dictionary, candidates)

    // Проверяем совпадения (Map: VacancyID -> DictionaryID)
    result.matches should contain (1 -> 10L)

    // Проверяем новые сущности (только значения)
    result.created should contain ("Python Developer")
    result.created should not contain "Java Developer"
  }

  it should "isolate matches by parent_id" in {
    val dictionary = Seq((50L, "manager", 100))

    val candidates = Seq(
      (1, "Manager", 100),
      (2, "Manager", 200)
    )

    val result = executeMatcher(dictionary, candidates)

    result.matches should contain (1 -> 50L)
    result.matches.get(2) shouldBe empty

    result.created should contain ("Manager")
  }

  it should "perform self-matching (clustering) for new values" in {
    val dictionary = Seq.empty[(Long, String, Int)]

    val candidates = Seq(
      (1, "Data Scientist", 100),
      (2, "Data Science", 100),
      (3, "Go Developer", 100)
    )

    val result = executeMatcher(dictionary, candidates)

    result.matches shouldBe empty

    val dataCluster = result.mappings.filter(_.normValue.contains("data"))
    dataCluster.map(_.normValue).distinct.length shouldBe 2


    result.created.length shouldBe 3
    result.created.exists(_.contains("Go")) shouldBe true
    result.created.exists(_.contains("Data")) shouldBe true
  }


  case class TestResult(
                         matches: Map[Int, Long],
                         created: Seq[String],
                         mappings: Seq[MappingRow]
                       )
  case class MappingRow(normValue: String, isCanonical: Boolean, rawValue: String)

  private def executeMatcher(
                              dictData: Seq[(Long, String, Int)],
                              candData: Seq[(Int, String, Int)]
                            ): TestResult = {

    val dictDf = dictData.toDF(dId, dVal, dPid)

    val candDf = candData.toDF(cId, cVal, cPid)
      .withColumn(cNorm, matcher.normCol(col(cVal)))

    val rawRes = matcher.execute(
      candDf, dictDf,
      cId, cVal, cNorm, cPid,
      dId, dVal, dPid
    )

    val matchesMap = rawRes.matchedDf.collect()
      .map(r => r.getAs[Int](cId) -> r.getAs[Long](dId))
      .toMap

    val createdList = rawRes.toCreateDf.collect()
      .map(r => r.getAs[String](cVal))
      .toSeq

    val mappingList = rawRes.mappingDataDf.collect()
      .map(r => MappingRow(
        r.getAs[String](cNorm),
        r.getAs[Boolean](rawRes.isCanonicalCol),
        r.getAs[String](cVal)
      ))
      .toSeq

    TestResult(matchesMap, createdList, mappingList)
  }
}