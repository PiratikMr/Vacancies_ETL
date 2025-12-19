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

//object TableRegistry {
//
//  val defaults: Map[FolderName, TableConfig] = Map(
//
//    FolderName.Skills -> TableConfig.list(),
//    FolderName.Roles -> TableConfig.dictionary(updates = Seq("name", "field_id")),
//    FolderName.Fields -> TableConfig.list(),
//    FolderName.Locations -> TableConfig.list()
//
//
//
////    FolderName.Areas -> TableConfig(
////      conflicts = Seq("id"),
////      updateStrategy = ExplicitUpdates(Seq("name"))
////    ),
////
////    FolderName.Currency -> TableConfig(
////      conflicts = Seq("id"),
////      updateStrategy = ExplicitUpdates(Seq("rate"))
////    ),
////
////    FolderName.Employment -> TableConfig.dictionary(),
////    FolderName.Experience -> TableConfig.dictionary(),
////    FolderName.Roles -> TableConfig.dictionary(updates = Seq("name", "field_id")),
////    FolderName.Schedule -> TableConfig.dictionary(),
////
////    FolderName.Employers -> TableConfig(
////      conflicts = Seq("id"),
////      updateStrategy = ExplicitUpdates(Seq("name", "trusted"))
////    ),
////
////    FolderName.Languages -> TableConfig.list(Seq("id", "name", "level"))
////    FolderName.JobFormats -> TableConfig.list(),
////    FolderName.Levels -> TableConfig.list(),
////
//
//  )
//
//  def get(folderName: FolderName): TableConfig = {
//    defaults.getOrElse(folderName, TableConfig(Seq("id"), DoNothing))
//  }
//}
