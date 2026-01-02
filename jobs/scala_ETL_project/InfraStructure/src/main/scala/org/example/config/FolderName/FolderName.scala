package org.example.config.FolderName

case class FolderName private (fullPath: String, folderName: String)


object FolderName extends FolderNames {
  def apply(layer: Layer, domain: Domain, folderName: String): FolderName = {
    construct(layer, domain, folderName)
  }

  def apply(layer: Layer, folderName: String): FolderName = {
    construct(layer, Empty, folderName)
  }

  private def construct(
                         layer: Layer,
                         domain: Domain,
                         folderName: String
                       ): FolderName = {
    val parts = Seq(
      layer.name,
      domain.name,
      folderName
    )

    new FolderName(parts.flatten.mkString("", "/", ""), folderName)
  }
}

sealed trait FolderNames {
  val Vacancies = FolderName(Stage, "Vacancies")
  val RawVacancies = FolderName(Raw, "Vacancies")

  val Skills = FolderName(Stage, "Skills")
  val Fields = FolderName(Stage, "Fields")
  val Locations = FolderName(Stage, "Locations")
}