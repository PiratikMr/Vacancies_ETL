package org.example.config.TableConfig

case class TableConfig(
                      conflicts: Seq[String],
                      updateStrategy: UpdateStrategy = DoNothing
                      )

object TableConfig {

  def list(
          conflicts: Seq[String] = Seq("id", "name")
          ): TableConfig = {
    TableConfig(
      conflicts = conflicts
    )
  }

}
