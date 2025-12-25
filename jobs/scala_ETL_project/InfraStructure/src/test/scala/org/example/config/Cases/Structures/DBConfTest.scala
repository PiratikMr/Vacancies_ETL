package org.example.config.Cases.Structures

import org.example.config.FolderName.FolderName
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class DBConfTest extends AnyFlatSpec {

  private val testDbConf = DBConf(
    name = "testUser",
    pass = "testPass",
    host = "localhost",
    port = "5432",
    base = "testDB",
    platform = "testPlatform"
  )

  "DBConf" should "generate correct JDBC URL" in {
    testDbConf.url shouldBe "jdbc:postgresql://localhost:5432/testDB"
  }

  "DBConf" should "generate correct table name" in {
    testDbConf.getDBTableName(FolderName.stage("Vacancies")) shouldBe "testPlatform_vacancies"
  }

}