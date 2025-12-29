package org.example.finder.implement

import org.example.finder.FinderTestObjects
import org.example.finder.config.FinderFileConfig
import org.example.{TestObjects, TestUtils}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class FinderExtractorTest extends AnyFlatSpec {

  private val spark = TestObjects.spark
  import spark.implicits._

  private val fileConf = FinderFileConfig(4, 2, "last_day")
  private val extractor = new FinderExtractor(fileConf, "api", 1, 1)



  "extract" should "correctly build URLs and read json" in {

    val vacancies = (1 to 4).map(id => TestUtils.readFile(s"rawVacancy$id.json"))

    val actualDS = extractor.extract(spark, FinderTestObjects.webService).collect().mkString("\n")
    val expectedDS = vacancies.map(str => str.replace("\n", "")).mkString("\n")

    actualDS shouldBe expectedDS
  }

  "filterUnActiveVacancies" should "return only unactive vacancy ids" in {

    val allIDs = (1 to 4).map(_.toLong).toDF("id")

    val actualIDs = extractor.filterUnActiveVacancies(spark, allIDs, FinderTestObjects.webService)
    val expectedIDsSeq = Seq(2, 4).map(_.toLong)

    actualIDs.count() shouldBe expectedIDsSeq.size

    val actualIDsSeq = actualIDs.collect().map(_.getLong(0)).sorted.toSeq

    actualIDsSeq shouldBe expectedIDsSeq
  }

}
