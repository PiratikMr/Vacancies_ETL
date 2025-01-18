package com.Config

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object HDFSConfig extends LocalConfig {
  private lazy val hdfs: String = getString("hdfs.url")
  private lazy val currentDate: String = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE)

  def path(isRoot: Boolean, fileName: String = ""): String = {
    if (isRoot)
      s"$hdfs$fileName"
    else
      s"$hdfs$currentDate/$fileName"
  }
}
