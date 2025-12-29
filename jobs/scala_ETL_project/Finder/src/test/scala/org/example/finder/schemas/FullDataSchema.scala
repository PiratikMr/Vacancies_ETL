package org.example.finder.schemas

case class FullDataSchema(
                         vacancy: VacancySchema,
                         locations: Seq[LocationSchema],
                         fields: Seq[FieldSchema]
                         )
