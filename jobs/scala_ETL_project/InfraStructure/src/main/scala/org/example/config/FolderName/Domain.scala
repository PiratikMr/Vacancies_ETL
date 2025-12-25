package org.example.config.FolderName

trait Domain {
  def name: String
}

case object Dictionaries extends Domain { val name = "Dictionaries" }