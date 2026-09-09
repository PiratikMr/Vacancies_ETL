package org.example.core.config.database

class MappingDimDef(val entity: String) {
  val entityId = s"${entity}_id"
  val mappingId = "mapping_id"
  val mappedValue = "mapped_value"
  val isCanonical = "is_canonical"
  val isActive = "is_active"
  val origin = "origin"
  val linkScore = "link_score"

  val matchLogDef = new MatchLogDef(entity)

  val meta = TableMeta(
    s"mapping_dim_$entity",
    Seq(entityId, mappedValue)
  )

}

object MappingOrigin {
  val REFERENCE = "reference"
  val ETL = "etl"
  val NLP_MERGE = "nlp_merge"
  val MANUAL = "manual"
  val LEGACY = "legacy"
  val UNKNOWN = "unknown"
}
