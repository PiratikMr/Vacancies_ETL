package org.example.config.FolderName

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class FolderNameTest extends AnyFlatSpec {

  private val testFolderName = "TestName"

  "FolderName" should "construct correct path without Domain" in {
    val rawFolder = FolderName(Raw, testFolderName)
    rawFolder.fullPath shouldBe s"${Raw.name}/$testFolderName"
    rawFolder.folderName shouldBe testFolderName
  }

  "FolderName" should "construct correct paths for Stage Layer" in {
    val stageFolder = FolderName.stage(testFolderName)
    stageFolder.fullPath shouldBe s"${Stage.name}/$testFolderName"
    stageFolder.folderName shouldBe testFolderName
  }

  "FolderName" should "construct correct path with Domain" in {
    val rawFolder = FolderName(Raw, Dictionaries, testFolderName)
    rawFolder.fullPath shouldBe s"${Raw.name}/${Dictionaries.name}/$testFolderName"
    rawFolder.folderName shouldBe testFolderName
  }

}
