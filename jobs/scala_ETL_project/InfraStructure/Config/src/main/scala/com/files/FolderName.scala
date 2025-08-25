package com.files


class FolderName(val path: String) extends Serializable {
  def isDictionary: Boolean = false
  def getName: String = path
  def getDBName: String = getName.toLowerCase
}

object FolderName extends FolderNames {
  def apply(str: String): FolderName = new FolderName(str)
}


private case class Dictionary(name: String) extends FolderName(s"Dictionaries/$name") {
  override def isDictionary: Boolean = true
  override def getName: String = name
}



trait FolderNames {

  // common
  val RawVacancies: FolderName = FolderName("RawVacancies")
  val Vacancies: FolderName = FolderName("Vacancies")
  val Skills: FolderName = FolderName("Skills")

  // dictionaries
  val Areas: FolderName = Dictionary("Areas")
  val Currency: FolderName = Dictionary("Currency")
  val Employment: FolderName = Dictionary("Employment")
  val Experience: FolderName = Dictionary("Experience")
  val Roles: FolderName = Dictionary("ProfessionalRoles")
  val Schedule: FolderName = Dictionary("Schedule")

  // specific
  val Employers: FolderName = FolderName("Employers")
  val DriverLicenses: FolderName = FolderName("DriverLicenses")
  val Languages: FolderName = FolderName("Languages")
  val Locations: FolderName = FolderName("Locations")
  val JobFormats: FolderName = FolderName("JobFormats")
  val Fields: FolderName = FolderName("Fields")
  val Levels: FolderName = FolderName("Grades")

}
