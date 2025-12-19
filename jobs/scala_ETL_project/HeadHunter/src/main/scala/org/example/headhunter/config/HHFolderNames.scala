package org.example.headhunter.config

import org.example.config.FolderName.{FolderName, Stage}
import org.example.config.TableConfig.{TableConfig, UpdateAllExceptKeys}
import org.example.core.objects.LoadDefinition

case class HHFolderName(
                       stage: FolderName,
                       loadDefinition: LoadDefinition
                       )

object HHFolderNames {

  private def apply(name: String, tableConfig: TableConfig): HHFolderName = {
    val folderName = FolderName(Stage, name)

    HHFolderName(
      folderName,
      LoadDefinition(folderName, tableConfig)
    )
  }

  lazy val languages: HHFolderName = apply("Languages", TableConfig(Seq("id", "name", "level")))
  lazy val employers: HHFolderName = apply("Employers", TableConfig(Seq("id"), UpdateAllExceptKeys))
}
