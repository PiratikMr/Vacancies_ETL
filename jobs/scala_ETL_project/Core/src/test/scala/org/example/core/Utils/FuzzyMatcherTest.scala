package org.example.core.Utils

import org.apache.spark.sql.functions._
import org.example.TestObjects
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.example.core.normalization.service.matching.FuzzyMatcher
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

class FuzzyMatcherTest extends AnyFunSuiteLike with Matchers {

  private val spark = TestObjects.spark

  import spark.implicits._

  // Общие настройки для тестов
  val settings = FuzzyMatchSettings(score = 0.1, numberPenalty = 0.1)
  val matcher = new FuzzyMatcher(spark, settings)

  // Константы имен колонок
  val cEntityId = "vacancy_id"
  val cRawValue = "value"
  val cNormValue = "norm_value"
  val cParentId = "parent_id"

  val dId = "id"
  val dNormValue = "norm_value"
  val dParentId = "parent_id"

  test("FuzzyMatcher: Dictionary Matching (Exact and Fuzzy)") {
    // Словарь: "Java Developer" (id=10)
    val dictDf = Seq(
      (10L, "java developer", 100)
    ).toDF(dId, dNormValue, dParentId)

    // Кандидаты:
    // 1. "Java Developer" (Идеальное совпадение)
    // 2. "Java Dev" (Fuzzy совпадение, должно пройти порог 0.5)
    // 3. "Python" (Не должно сматчиться)
    val candidatesRaw = Seq(
      (1, "Java Developer", 100),
      (2, "Java Dev", 100),
      (3, "Python", 100)
    ).toDF(cEntityId, cRawValue, cParentId)

    val candidatesDf = candidatesRaw.withColumn(cNormValue, matcher.normCol(col(cRawValue)))

    val result = matcher.execute(
      candidatesDf, dictDf,
      cEntityId, cRawValue, cNormValue, cParentId,
      dId, dNormValue, dParentId
    )

    // Проверки
    val matches = result.matchedDf.collect()
    matches.length shouldBe 2 // Java Developer и Java Dev должны найтись в словаре

    val javaMatch = matches.find(r => r.getAs[Int](cEntityId) == 1).get
    javaMatch.getAs[Long](dId) shouldBe 10L

    val fuzzyMatch = matches.find(r => r.getAs[Int](cEntityId) == 2).get
    fuzzyMatch.getAs[Long](dId) shouldBe 10L

    // Python должен уйти в toCreateDf
    val toCreate = result.toCreateDf.collect()
    toCreate.length shouldBe 1
    toCreate.head.getAs[String](cRawValue) shouldBe "Python"
  }

  test("FuzzyMatcher: Parent ID Isolation") {
    // Словарь: "Manager" в parent_id=100
    val dictDf = Seq(
      (50L, "manager", 100)
    ).toDF(dId, dNormValue, dParentId)

    // Кандидаты: "Manager" в parent_id=100 (должен сматчиться) и "Manager" в parent_id=200 (НЕ должен)
    val candidatesRaw = Seq(
      (1, "Manager", 100),
      (2, "Manager", 200)
    ).toDF(cEntityId, cRawValue, cParentId)

    val candidatesDf = candidatesRaw.withColumn(cNormValue, matcher.normCol(col(cRawValue)))

    val result = matcher.execute(
      candidatesDf, dictDf,
      cEntityId, cRawValue, cNormValue, cParentId,
      dId, dNormValue, dParentId
    )

    // Проверка матчей
    val matches = result.matchedDf.collect()
    matches.length shouldBe 1
    matches.head.getAs[Int](cEntityId) shouldBe 1 // Только тот, что с parent_id=100

    // Проверка создания
    val toCreate = result.toCreateDf.collect()
    toCreate.exists(r => r.getAs[Int](cEntityId) == 2) shouldBe true // Manager c id=200 должен быть создан как новый
  }

  test("FuzzyMatcher: Self-Matching (Clustering)") {
    val dictDf = spark.emptyDataFrame.withColumn(dId, lit(1L)).withColumn(dNormValue, lit("")).withColumn(dParentId, lit(1))

    // Сценарий: "Data Scientist" и "Data Science" должны склеиться.
    // "Data Scientist" длиннее и лексикографически позже/раньше, но логика ранжирования выберет одного.
    // "Go" - уникальный.
    val candidatesRaw = Seq(
      (1, "Data Scientist", 100),
      (2, "Data Science", 100),
      (3, "Go", 100)
    ).toDF(cEntityId, cRawValue, cParentId)

    val candidatesDf = candidatesRaw.withColumn(cNormValue, matcher.normCol(col(cRawValue)))

    val result = matcher.execute(
      candidatesDf, dictDf,
      cEntityId, cRawValue, cNormValue, cParentId,
      dId, dNormValue, dParentId
    )

    // 1. Никто не сматчился со словарем
    result.matchedDf.count() shouldBe 0

    // 2. Mapping Data (Связи синонимов)
    // Один из них (Data Scientist или Data Science) станет каноническим, второй запишется в маппинг.
    // Плюс "Go" считается каноническим сам для себя (но в mappingData попадают неканонические -> канонические,
    // либо структура возвращает маппинг для сохранения синонимов).
    // По логике кода: `newMappingData` содержит `isCanonical`.

    val mappingData = result.mappingDataDf.collect()
    // Должно быть 3 записи (каждый мапится либо сам на себя как каноник, либо на другого)
    // В коде selfMatching возвращает distinct select, включая isCanonical

    val dsItems = mappingData.filter(r => r.getAs[String](cRawValue).contains("Data"))
    dsItems.length shouldBe 2

    result.mappingDataDf.show()

    // Проверяем, что один из них canonical = true, другой = false, и norm_value у них ОДИНАКОВОЕ (ссылаются на один хаб)
    val canonicalDS = dsItems.find(_.getAs[Boolean]("is_canonical"))
    val synonymDS = dsItems.find(!_.getAs[Boolean]("is_canonical"))

    assert(canonicalDS.isDefined, "Должен быть выбран канонический вариант для Data Science")
    assert(synonymDS.isDefined, "Должен быть определен синоним для Data Science")



    // Проверка, что синоним ссылается на норму каноника
    synonymDS.get.getAs[String](cRawValue) shouldBe canonicalDS.get.getAs[String](cRawValue)

    // 3. To Create (Что сохранять в dim таблицу)
    // Должны сохранить только Канонические значения ("Go" и победителя из Data Science)
    val toCreate = result.toCreateDf.collect()
    toCreate.length shouldBe 3
    toCreate.map(_.getAs[String](cRawValue)).toSet should contain("Go")
    toCreate.map(_.getAs[String](cRawValue)).toSet should contain(canonicalDS.get.getAs[String](cRawValue))
  }
}