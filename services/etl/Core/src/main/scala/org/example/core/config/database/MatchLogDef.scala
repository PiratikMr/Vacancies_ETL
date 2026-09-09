package org.example.core.config.database

class MatchLogDef(val entity: String) {
  val vacancyId = "vacancy_id"
  val mappingId = "mapping_id"
  val rawValue = "raw_value"
  val score = "score"

  val meta = TableMeta(
    s"match_log_$entity",
    Seq(vacancyId, mappingId, rawValue)
  )
}
