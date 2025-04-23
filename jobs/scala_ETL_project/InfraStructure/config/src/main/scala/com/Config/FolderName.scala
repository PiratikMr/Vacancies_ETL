package com.Config

object FolderName {
  type FolderName = String
  def isDict(folderName: FolderName): Boolean = folderName == Dict

  private var Dict = ""
  def Dict(folderName: FolderName): FolderName = {
    Dict = s"Dictionaries/$folderName"
    Dict
  }

  // General

  val Raw = "RawVacancies"
  val Vac = "Vacancies"
  val Skills = "Skills"

  // HeadHunter

  val Employer = "Employers"
  val Areas = "Areas"
  val Roles = "Roles"
  val Currency = "Currency"
  val Schedule = "Schedule"
  val Employment = "Employment"
  val Experience = "Experience"


  // GetMatch -- GeekJOB

  val Locations = "Locations"

  // GeekJOB

  val JobFormats = "JobFormat"
  val Fields = "Fields"
  val Levels = "Level"

}
