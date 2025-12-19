package org.example.currency.implement

import org.example.config.FolderName.{FolderName, Stage}
import org.example.config.TableConfig.{TableConfig, UpdateAllExceptKeys}
import org.example.core.Interfaces.ETL.Loader
import org.example.core.objects.LoadDefinition

object CurrencyLoader extends Loader {

  override def tablesToLoad(): Seq[LoadDefinition] = {
    Seq(
      LoadDefinition(
        FolderName(Stage, "Currency"),
        TableConfig(Seq("id"), UpdateAllExceptKeys)
      )
    )
  }
}
