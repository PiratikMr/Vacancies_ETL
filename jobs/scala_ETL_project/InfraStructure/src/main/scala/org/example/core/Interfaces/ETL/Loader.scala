package org.example.core.Interfaces.ETL

import org.example.core.objects.LoadDefinition

trait Loader {

  def tablesToLoad(): Seq[LoadDefinition]

}
