package org.example.core.objects

import org.example.core.config.schema.SchemaRegistry.DataBase.{Entities, FactVacancy}

object NormalizersEnum {

  sealed trait NormalizerType {
    def mappedIdCol: String
  }

  sealed trait GroupNonHierarchical extends NormalizerType
  sealed trait GroupHierarchical extends NormalizerType

  case object CURRENCY    extends GroupNonHierarchical { override def mappedIdCol: String = FactVacancy.currencyId.name }
  case object EMPLOYER    extends GroupNonHierarchical { override def mappedIdCol: String = FactVacancy.employerId.name }
  case object EXPERIENCE  extends GroupNonHierarchical { override def mappedIdCol: String = FactVacancy.experienceId.name }
  case object PLATFORM    extends GroupNonHierarchical { override def mappedIdCol: String = FactVacancy.platformId.name }
  case object EMPLOYMENTS extends GroupNonHierarchical { override def mappedIdCol: String = Entities.Employments.bridge.entityId.name }
  case object FIELDS      extends GroupNonHierarchical { override def mappedIdCol: String = Entities.Fields.bridge.entityId.name }
  case object GRADES      extends GroupNonHierarchical { override def mappedIdCol: String = Entities.Grades.bridge.entityId.name }
  case object SCHEDULES   extends GroupNonHierarchical { override def mappedIdCol: String = Entities.Schedules.bridge.entityId.name }
  case object SKILLS      extends GroupNonHierarchical { override def mappedIdCol: String = Entities.Skills.bridge.entityId.name }

  case object COUNTRY     extends GroupNonHierarchical { override def mappedIdCol: String = Entities.Locations.dimTable.parentId.name }
  case object LOCATIONS   extends GroupHierarchical { override def mappedIdCol: String = Entities.Locations.bridge.entityId.name }

  case object LANGUAGES   extends GroupNonHierarchical { override def mappedIdCol: String = Entities.Languages.bridge.entityId.name }
  case object LANGUAGES_LEVEL extends GroupNonHierarchical { override def mappedIdCol: String = Entities.Languages.levelId.name }




  val values: Set[NormalizerType] = Set(
    CURRENCY, EMPLOYER, EXPERIENCE, PLATFORM, EMPLOYMENTS,
    FIELDS, GRADES, SCHEDULES, SKILLS, COUNTRY, LOCATIONS, LANGUAGES,
    LANGUAGES_LEVEL
  )

  lazy val nonHierarchical: Set[GroupNonHierarchical] = values.collect { case x: GroupNonHierarchical => x }
  lazy val hierarchical: Set[GroupHierarchical] = values.collect { case x: GroupHierarchical => x }
}
