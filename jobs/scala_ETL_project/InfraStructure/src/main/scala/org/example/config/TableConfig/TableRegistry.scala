package org.example.config.TableConfig

import org.example.config.FolderName.FolderName
import org.example.core.objects.LoadDefinition

object TableRegistry {

  val Vacancies = LoadDefinition(FolderName.Vacancies, TableConfig(
    conflicts = Seq("id"),
    updateStrategy = UpdateAllExceptKeys
  ))

  val Locations = LoadDefinition(FolderName.Locations, TableConfig.list())
  val Fields = LoadDefinition(FolderName.Fields, TableConfig.list())
  val Skills = LoadDefinition(FolderName.Skills, TableConfig.list())
}