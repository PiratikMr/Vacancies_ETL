package org.example.config.FolderName

case class FolderName private (fullPath: String, folderName: String)


object FolderName extends FolderNames {
  def apply(layer: Layer, domain: Domain, folderName: String): FolderName = {
    construct(folderName, layer, Some(domain))
  }
  def apply(layer: Layer, folderName: String): FolderName = {
    construct(folderName, layer, None)
  }

  def stage(folderName: String): FolderName = {
    construct(folderName, Stage, None)
  }

  private def construct(
                         folderName: String,
                         layer: Layer,
                         domain: Option[Domain],
                       ): FolderName = {
    val parts = Seq(
      Some(layer.name),
      domain.map(_.name),
      Some(folderName)
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