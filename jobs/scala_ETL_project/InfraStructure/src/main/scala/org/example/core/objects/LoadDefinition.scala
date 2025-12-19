package org.example.core.objects

import org.example.config.FolderName.FolderName
import org.example.config.TableConfig.TableConfig

case class LoadDefinition(
                         folder: FolderName,
                         config: TableConfig
                         )
