package org.example.core.Utils.NormalizeService

import org.apache.spark.sql.DataFrame
import org.example.TestObjects
import org.example.core.adapter.database.DataBaseAdapter
import org.example.core.config.model.structures.FuzzyMatchSettings
import org.example.core.normalization.config.{DimTableConf, MappingDimTableConf}
import org.example.core.normalization.service.NormalizeService
// Импорты для Mockito Core + ScalaTestPlus
import org.mockito.ArgumentMatchers.{any, anyString, eq => eqTo}
import org.mockito.Mockito.{when => mockWhen, _}
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class NormalizeServiceTest extends AnyFunSuiteLike with Matchers with MockitoSugar {

  private val spark = TestObjects.spark
  import spark.implicits._

  // Конфиги (заглушки для теста)
  val dt = DimTableConf("dim_skills", "id", "value", Some("parent_id"))
  val mdt = MappingDimTableConf("map_skills", "id", "norm_value", "is_origin")
  val settings = FuzzyMatchSettings(score = 0.6, numberPenalty = 0.1)

  // Хелпер для создания DataFrame маппинга
  def createMappingDf(rows: Seq[(Long, String, Boolean, Int)]): DataFrame = {
    rows.toDF("id", "norm_value", "is_origin", "parent_id")
  }

  test("NormalizeService: extractTags should find keywords in text") {
    // 1. Создаем мок через MockitoSugar
    val dbAdapter = mock[DataBaseAdapter]
    val service = new NormalizeService(spark, dbAdapter, settings, dt, mdt)

    // 2. Настраиваем поведение мока (when ... thenReturn)
    // Используем any() для SparkSession и строки запроса
    val mappingData = Seq(
      (10L, "java", true, 100),
      (20L, "spark", true, 100)
    )
    mockWhen(dbAdapter.loadQuery(any(), anyString())).thenReturn(createMappingDf(mappingData))

    // 3. Входные данные
    val input = Seq(
      (1, "Looking for a java developer with spark knowledge"),
      (2, "Just a manager")
    ).toDF("vac_id", "desc")

    // 4. Выполнение
    val res = service.extractTags(input, "vac_id", "desc")

    // 5. Проверки
    val rows = res.mappedDf.collect()
    // Должно найти 2 тега для вакансии 1
    rows.count(r => r.getAs[Int]("vac_id") == 1) shouldBe 2

    val ids = rows.filter(r => r.getAs[Int]("vac_id") == 1).map(_.getAs[Long]("id")).toSet
    ids should contain(10L)
    ids should contain(20L)

    rows.count(r => r.getAs[Int]("vac_id") == 2) shouldBe 0
  }

  test("NormalizeService: mapSimple - Existing Mapping") {
    val dbAdapter = mock[DataBaseAdapter]
    val service = new NormalizeService(spark, dbAdapter, settings, dt, mdt)

    // Мок: маппинг уже содержит "senior" -> 555
    mockWhen(dbAdapter.loadQuery(any(), anyString())).thenReturn(
      createMappingDf(Seq((555L, "senior", true, 100)))
    )

    val input = Seq((1, "Senior", 100)).toDF("vac_id", "val", "pid")

    // Аргумент withCreate передаем явно true, это примитив
    val res = service.mapSimple(input, "vac_id", "val", Some("pid"), withCreate = true)

    val rows = res.mappedDf.collect()
    rows.length shouldBe 1
    rows.head.getAs[Long]("id") shouldBe 555L

    // Проверяем, что сохранение НЕ вызывалось (never)
    // Важно: типизация Seq в matchers может быть капризной, используем any() для коллекций
    verify(dbAdapter, never).save(any(), anyString(), any())
  }

  test("NormalizeService: mapSimple - New Value Creation") {
    val dbAdapter = mock[DataBaseAdapter]
    val service = new NormalizeService(spark, dbAdapter, settings, dt, mdt)

    // 1. Пустой маппинг
    mockWhen(dbAdapter.loadQuery(any(), anyString())).thenReturn(
      createMappingDf(Seq.empty)
    )

    // 2. Мокаем saveWithReturn. Имитируем возврат нового ID=999
    val returnedDim = Seq((999L, "Kotlin", 100)).toDF("id", "value", "parent_id")

    // eqTo используем для проверки имени таблицы, остальное any()
    mockWhen(dbAdapter.saveWithReturn(any(), any(), eqTo(dt.tableName), any(), any()))
      .thenReturn(returnedDim)

    val input = Seq((1, "Kotlin", 100)).toDF("vac_id", "val", "pid")

    val res = service.mapSimple(input, "vac_id", "val", Some("pid"), withCreate = true)

    val rows = res.mappedDf.collect()
    rows.length shouldBe 1
    rows.head.getAs[Long]("id") shouldBe 999L

    // 3. Должен был сохраниться маппинг (синоним)
    verify(dbAdapter, atLeastOnce).save(any(), eqTo(mdt.tableName), any())
  }

  test("NormalizeService: mapSimple - Fuzzy Match to Existing") {
    val dbAdapter = mock[DataBaseAdapter]
    val service = new NormalizeService(spark, dbAdapter, settings, dt, mdt)

    // 1. Маппинг содержит "kubernet" (норма для Kubernetes) -> id 700
    // Примечание: в тестах мы не проверяем саму нормализацию (это задача FuzzyMatcherTest),
    // но мы знаем, что FuzzyMatcher нормализует "Kubernetess" похоже на "kubernet".
    // Для надежности теста добавим в "БД" именно то, во что превратится "Kubernetess"
    // Допустим, стеммер превратит это в "kubernet"
    val normK8s = "kubernet"
    mockWhen(dbAdapter.loadQuery(any(), anyString())).thenReturn(
      createMappingDf(Seq((700L, normK8s, true, 100)))
    )

    val input = Seq((1, "Kubernetess", 100)).toDF("vac_id", "val", "pid")

    val res = service.mapSimple(input, "vac_id", "val", Some("pid"), withCreate = true)

    val rows = res.mappedDf.collect()
    rows.length shouldBe 1
    rows.head.getAs[Long]("id") shouldBe 700L

    // Проверяем, что было сохранение в таблицу маппинга (добавилась опечатка как синоним)
    verify(dbAdapter, times(1)).save(any(), eqTo(mdt.tableName), any())
    // Но не создавалась новая сущность
    verify(dbAdapter, never).saveWithReturn(any(), any(), any(), any(), any())
  }
}