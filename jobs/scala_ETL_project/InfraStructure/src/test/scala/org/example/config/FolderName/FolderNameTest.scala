package org.example.config.FolderName

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class FolderNameTest extends AnyFlatSpec {

  private val testFolderName = "TestName"

  "FolderName" should "construct correct path" in {
    val rawFolder = FolderName(Raw, Dictionaries, testFolderName)
    rawFolder.fullPath shouldBe s"${Raw.name}/${Dictionaries.name}/$testFolderName"
    rawFolder.folderName shouldBe testFolderName
  }

}
