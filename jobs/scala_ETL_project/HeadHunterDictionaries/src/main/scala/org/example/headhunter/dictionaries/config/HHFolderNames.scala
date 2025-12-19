package org.example.headhunter.dictionaries.config

import org.example.config.FolderName.{Dictionaries, FolderName, Raw, Stage}
import org.example.config.TableConfig.{ExplicitUpdates, TableConfig}
import org.example.core.objects.LoadDefinition

case class HHFolderName(
                        raw: FolderName,
                        stage: FolderName,
                        loadDefinition: LoadDefinition
                        )

object HHFolderNames {

  private def apply(name: String, tableConfig: TableConfig): HHFolderName = {
    val stage = FolderName(Stage, Dictionaries, name)

    HHFolderName(
      FolderName(Raw, Dictionaries, name),
      stage,
      LoadDefinition(stage, tableConfig)
    )
  }

  private val defTableConfig: TableConfig = TableConfig(Seq("id"), ExplicitUpdates(Seq("name")))

  lazy val areas: HHFolderName = apply("Areas", defTableConfig)
  lazy val roles: HHFolderName = apply("ProfessionalRoles",
    TableConfig(Seq("id"), ExplicitUpdates(Seq("name", "field_id")))
  )
  lazy val schedule: HHFolderName = apply("Schedule", defTableConfig)
  lazy val employment: HHFolderName = apply("Employment", defTableConfig)
  lazy val experience: HHFolderName = apply("Experience", defTableConfig)
}
