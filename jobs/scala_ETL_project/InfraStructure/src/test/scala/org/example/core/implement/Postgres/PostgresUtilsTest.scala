package org.example.core.implement.Postgres

import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.example.TestObjects
import org.example.config.Cases.Structures.DBConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper


class PostgresUtilsTest extends AnyFlatSpec with ForAllTestContainer {

  override val container: PostgreSQLContainer = PostgreSQLContainer(
    dockerImageNameOverride = "postgres:17-alpine"
  ).configure { c =>
    c.withInitScript("init.sql")
  }

  private lazy val dbConf: DBConf = DBConf(
    container.username,
    container.password,
    container.host,
    container.mappedPort(5432).toString,
    container.databaseName,
    "testPlatform"
  )

  private val spark = TestObjects.spark
  import spark.implicits._

  "PostgresUtils" should "create table and save data" in {

    val tableName = "testTable"

    val inputData = Seq(
      (1, "NewUser")
    ).toDF("id", "name")

    PostgresUtils.save(dbConf, inputData, tableName, Seq("id"))
    val result = PostgresUtils.loadHelper(spark, dbConf, Seq("dbtable" -> tableName))

    assertSchemaEquals(inputData.schema, result.schema)
    assertDataEquals(inputData, result)
  }

  "PostgresUtils" should "perform UPSERT correctly" in {
    val tableName = "test_upsert_table"

    val initialData = Seq(
      (1, "Alice", "Designer"),
      (5, "Bob", "Analytic")
    ).toDF("id", "name", "role")

    val updates = Seq(
      (1, "Alice", "Software Developer"),
      (5, "Garmin", "Analytic"),
      (6, "Carol", "HR")
    ).toDF("id", "name", "role")

    val expectedData = updates

    PostgresUtils.save(dbConf, initialData, tableName, Seq("id"))
    PostgresUtils.save(dbConf, updates, tableName, Seq("id"), Some(Seq("name", "role")))

    val actualResultFromDB = PostgresUtils.loadHelper(spark, dbConf, Seq("dbtable" -> tableName))

    assertSchemaEquals(actualResultFromDB.schema, expectedData.schema)
    assertDataEquals(actualResultFromDB, expectedData)
  }


  private def assertSchemaEquals(actual: StructType, expected: StructType): Unit = {
    actual.length shouldBe expected.length

    actual.zip(expected).foreach { case (fieldActual, fieldExpected) =>
      fieldActual.name shouldBe fieldExpected.name
      fieldActual.dataType shouldBe fieldExpected.dataType
    }
  }

  private def assertDataEquals(actual: DataFrame, expected: DataFrame): Unit = {
    val actualRows = actual.collect().sortBy(_.getInt(0)).toList
    val expectedRows = expected.collect().sortBy(_.getInt(0)).toList

    actualRows shouldBe expectedRows
  }
}
