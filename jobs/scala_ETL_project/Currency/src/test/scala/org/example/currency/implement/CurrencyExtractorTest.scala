package org.example.currency.implement

import org.example.core.Interfaces.Services.WebService
import org.example.{TestObjects, TestUtils}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class CurrencyExtractorTest extends AnyFlatSpec {


  private val spark = TestObjects.spark

  private val webService = new WebService {
    override def read(url: String): Either[String, String] = ???

    override def readOrDefault(url: String, default: String): String = ???

    override def readOrNone(url: String): Option[String] = ???

    override def readOrThrow(url: String): String =
      TestUtils.readFile("rawData.json")
  }


  private val extractor = new CurrencyExtractor("", "")


  "extract" should "get json and remove pretty print" in {
    val ds = extractor.extract(spark, webService)

    val expectedData = TestUtils.readFile("rawData.json").replace("\n", "")

    ds.count() shouldBe 1
    ds.first() shouldBe expectedData
  }
}