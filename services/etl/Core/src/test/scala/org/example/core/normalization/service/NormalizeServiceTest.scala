package org.example.core.normalization.service

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.example.SparkEnv
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.database.{DimDef, MappingDimDef}
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.example.core.normalization.engine.FuzzyMatcher
import org.example.core.normalization.engine.model._
import org.example.core.normalization.model.{NormCandidate, NormalizationColumns}
import org.mockito.ArgumentMatchers.{any, eq => eqTo}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.mockito.MockitoSugar

class NormalizeServiceTest extends AnyFlatSpec with SparkEnv with MockitoSugar {

  import spark.implicits._

  private val dbAdapterMock = mock[DataBaseAdapter]
  private val fuzzyMatcherMock = mock[FuzzyMatcher]

  private val settings = FuzzyMatchSettings(0.7, 1.0, 0.5, 0.5, 3)
  private val dimDef = new DimDef("test", Some("parent_id"))
  private val mappingDef = new MappingDimDef("test")

  private val normalizeService = new NormalizeService(
    spark,
    dbAdapterMock,
    settings,
    dimDef,
    mappingDef,
    Some(fuzzyMatcherMock)
  )

  "NormalizeService" should "extract tags utilizing DataBaseAdapter and FuzzyMatcher" in {
    val fakeMappingData = Seq(
      (100L, 1000L, "fake_norm", true, 1L)
    ).toDF(
      "id", "mapping_id", "norm_value", "is_canonical", "parent_id"
    )

    when(dbAdapterMock.loadQuery(any[SparkSession], any[String])).thenReturn(fakeMappingData)

    val fakeMatchDf = Seq(
      FuzzyMatch("candidate1", 100L, 1000L, "some value", FuzzyScores.EXACT)
    ).toDS()

    when(fuzzyMatcherMock.extractTags(any[Dataset[FuzzyCandidate]], any[Dataset[FuzzyDictionary]]))
      .thenReturn(fakeMatchDf)

    val candidates = Seq(NormCandidate("candidate1", "some value", Some("1"))).toDS()
    val res = normalizeService.extractTags(candidates)

    val matches = res.matches.collect()
    matches.length shouldBe 1
    matches.head.entityId shouldBe "candidate1"
    matches.head.mappedId shouldBe 100L

    val log = res.log.collect()
    log.length shouldBe 1
    log.head.entityId shouldBe "candidate1"
    log.head.mappingId shouldBe 1000L
    log.head.rawValue shouldBe "some value"
    log.head.score shouldBe FuzzyScores.EXACT

    verify(dbAdapterMock, times(1)).loadQuery(any[SparkSession], any[String])
    verify(fuzzyMatcherMock, times(1)).extractTags(any[Dataset[FuzzyCandidate]], any[Dataset[FuzzyDictionary]])
  }

  "NormalizeService" should "mapSimple creating new dim entries" in {
    val emptyMappingData = spark.emptyDataset[(Long, Long, String, Boolean, Long)].toDF(
      "id", "mapping_id", "norm_value", "is_canonical", "parent_id"
    )
    when(dbAdapterMock.loadQuery(any[SparkSession], any[String])).thenReturn(emptyMappingData)

    val mockRes = FuzzyMatcherResult(
      matched = spark.emptyDataset[FuzzyMatch],
      toCreate = Seq(FuzzyToCreate("cand1", "val1", "val1", "nval1", 1L, FuzzyScores.EXACT)).toDS(),
      mappingData = Seq(FuzzyMappingMeta("val1", "nval1", isCanonical = true, 1L, FuzzyScores.EXACT)).toDS(),
      clearCache = () => {}
    )

    when(fuzzyMatcherMock.execute(any[Dataset[FuzzyCandidate]], any[Dataset[FuzzyDictionary]]))
      .thenReturn(mockRes)

    val returnedDimDf = Seq(
      (200L, "val1", 1L)
    ).toDF("id", NormalizationColumns.RAW_VALUE, "parent_id")

    val returnedMappingDf = Seq(
      (300L, 200L, "nval1")
    ).toDF("mapping_id", "test_id", "mapped_value")

    when(dbAdapterMock.saveWithReturn(
      any[SparkSession], any[DataFrame], eqTo(dimDef.meta.tableName), any[Seq[String]], any[Seq[String]], any[Option[Seq[String]]]
    )).thenReturn(returnedDimDf)

    when(dbAdapterMock.saveWithReturn(
      any[SparkSession], any[DataFrame], eqTo(mappingDef.meta.tableName), any[Seq[String]], any[Seq[String]], any[Option[Seq[String]]]
    )).thenReturn(returnedMappingDf)

    val candidates = Seq(NormCandidate("cand1", "val1", Some("1"))).toDS()
    val res = normalizeService.mapSimple(candidates, withCreate = true)

    val matches = res.matches.collect()
    matches.length shouldBe 1
    matches.head.entityId shouldBe "cand1"
    matches.head.mappedId shouldBe 200L

    val log = res.log.collect()
    log.length shouldBe 1
    log.head.entityId shouldBe "cand1"
    log.head.mappingId shouldBe 300L
    log.head.rawValue shouldBe "val1"

    verify(dbAdapterMock, times(1)).saveWithReturn(
      any[SparkSession], any[DataFrame], eqTo(dimDef.meta.tableName), any[Seq[String]], any[Seq[String]], any[Option[Seq[String]]]
    )
    verify(dbAdapterMock, times(1)).saveWithReturn(
      any[SparkSession], any[DataFrame], eqTo(mappingDef.meta.tableName), any[Seq[String]], any[Seq[String]], any[Option[Seq[String]]]
    )
    verify(dbAdapterMock, never()).save(
      any[DataFrame], any[String], any[Seq[String]], any[Option[Seq[String]]]
    )
  }

}
