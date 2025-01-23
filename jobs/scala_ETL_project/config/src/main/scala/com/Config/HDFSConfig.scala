package com.Config

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object HDFSConfig extends LocalConfig {
  private lazy val hdfs: String = getStringField("HDFS.url")
  private lazy val currentDate: String = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE)

  lazy val vacanciesRawFileName: String = getStringField("HDFS.fileName.vacanciesRaw")
  lazy val vacanciesTransformedFileName: String = getStringField("HDFS.fileName.vacanciesTransformed")

  def path(isRoot: Boolean, fileName: String = ""): String = {
    if (isRoot)
      s"$hdfs$fileName"
    else
      s"$hdfs$currentDate/$fileName"
  }
}
