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



//
//
//class FolderName(val path: String) extends Serializable {
//  def isDictionary: Boolean = false
//  def getName: String = path
//}
//
//object FolderName extends FolderNames {
//  def apply(str: String): kek.FolderName = new kek.FolderName(str)
//}
//
//case class Dictionary(name: String) extends kek.FolderName(s"Dictionaries/$name") {
//  override def isDictionary: Boolean = true
//  override def getName: String = name
//}
//
//
//
//trait FolderNames {
//
//  // common
//
//  // dictionaries
//  val RawDictionaries: kek.FolderName = Dictionary("raw")
//
//  val Areas: kek.FolderName = Dictionary("Areas")
//  val RawAreas: kek.FolderName = Dictionary("RawAreas")
//
//  val Currency: kek.FolderName = Dictionary("Currency")
//  val Employment: kek.FolderName = Dictionary("Employment")
//  val Experience: kek.FolderName = Dictionary("Experience")
//
//  val RawRoles: kek.FolderName = Dictionary("RawProfessionalRoles")
//
//  val Schedule: kek.FolderName = Dictionary("Schedule")
//
//  // specific
//  val Employers: kek.FolderName = FolderName("Employers")
//  val DriverLicenses: kek.FolderName = FolderName("DriverLicenses")
//  val Languages: kek.FolderName = FolderName("Languages")
//  val Locations: kek.FolderName = FolderName("Locations")
//  val JobFormats: kek.FolderName = FolderName("JobFormats")
//  val Levels: kek.FolderName = FolderName("Grades")
//
//}
