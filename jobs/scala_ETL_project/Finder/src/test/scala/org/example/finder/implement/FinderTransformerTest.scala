package org.example.finder.implement

import org.example.config.FolderName.FolderName
import org.example.finder.FinderTestUtils
import org.example.finder.schemas.{FieldSchema, LocationSchema, VacancySchema}
import org.example.{TestObjects, TestUtils}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class FinderTransformerTest extends AnyFlatSpec {

  private val spark = TestObjects.spark
  import spark.implicits._

  private val transformer = new FinderTransformer(1)


  "transform" should "return correctly transformed DataFrames" in {
    val inputData = (1 to 4).map(id => s"rawVacancy$id.json")
      .map(path => TestUtils.readFile(path))
      .map(_.replace("\n", ""))
      .toDS()

    val expectedValues = (1 to 4).map(id => FinderTestUtils.createDataRow(id))


    val rows = transformer.toRows(spark, inputData)
    val actualDFs = transformer.transform(spark, rows)

    val expectedVacancies = expectedValues.map(_.vacancy)
    val expectedLocations = expectedValues.flatMap(_.locations)
    val expectedFields = expectedValues.flatMap(_.fields)

    val actualVacancies = actualDFs(FolderName.Vacancies).as[VacancySchema].sort("id").collect().toSeq
    val actualLocations = actualDFs(FolderName.Locations).as[LocationSchema].sort("id", "name").collect().toSeq
    val actualFields = actualDFs(FolderName.Fields).as[FieldSchema].sort("id", "name").collect().toSeq

    actualVacancies shouldBe expectedVacancies
    actualLocations shouldBe expectedLocations
    actualFields shouldBe expectedFields
  }
}
