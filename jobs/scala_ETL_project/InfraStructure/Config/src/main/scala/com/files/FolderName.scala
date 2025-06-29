package com.files

object FolderName {
  type FolderName = String

  // General

  val Raw = "RawVacancies"
  val Vac = "Vacancies"
  val Skills = "Skills"

  // HeadHunter

  private val dict: String = "Dictionaries"

  val Areas = s"$dict/Areas"
  val Currency = s"$dict/Currency"
  val Employment = s"$dict/Employment"
  val Experience = s"$dict/Experience"
  val Roles = s"$dict/Roles"
  val Schedule = s"$dict/Schedule"

  val Employer = "Employers"

  def isDict(folderName: FolderName): Boolean = folderName.startsWith(dict)

  // GetMatch -- GeekJOB

  val Locations = "Locations"

  // GeekJOB

  val JobFormats = "JobFormat"
  val Fields = "Fields"
  val Levels = "Level"

}
