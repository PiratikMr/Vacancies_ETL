package org.example.config.FolderName

sealed trait Domain {
  def name: String
}

case class CustomDomain(name: String) extends Domain

case object Dictionaries extends Domain { val name = "Dictionaries" }
case object Empty extends Domain { val name = "-" }