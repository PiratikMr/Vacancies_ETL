package org.example.config.FolderName

trait Domain {
  def name: String
}

case class CustomDomain(name: String) extends Domain

case object Dictionaries extends Domain { val name = "Dictionaries" }