package org.example.core.normalization.model

import org.example.core.config.database._

object NormalizersEnum {

  sealed trait NormalizerType {
    def mappedIdCol: String
  }

  sealed trait GroupNonHierarchical extends NormalizerType
  sealed trait GroupHierarchical extends NormalizerType

  case object CURRENCY    extends GroupNonHierarchical { override def mappedIdCol: String = FactVacancyDef.currencyId }
  case object EMPLOYER    extends GroupNonHierarchical { override def mappedIdCol: String = FactVacancyDef.employerId }
  case object EXPERIENCE  extends GroupNonHierarchical { override def mappedIdCol: String = FactVacancyDef.experienceId }
  case object PLATFORM    extends GroupNonHierarchical { override def mappedIdCol: String = FactVacancyDef.platformId }
  case object EMPLOYMENTS extends GroupNonHierarchical { override def mappedIdCol: String = BridgeVacancyEmploymentDef.entityId }
  case object FIELDS      extends GroupNonHierarchical { override def mappedIdCol: String = BridgeVacancyFieldDef.entityId }
  case object GRADES      extends GroupNonHierarchical { override def mappedIdCol: String = BridgeVacancyGradeDef.entityId }
  case object SCHEDULES   extends GroupNonHierarchical { override def mappedIdCol: String = BridgeVacancyScheduleDef.entityId }
  case object SKILLS      extends GroupNonHierarchical { override def mappedIdCol: String = BridgeVacancySkillDef.entityId }

  case object COUNTRY     extends GroupNonHierarchical { override def mappedIdCol: String = DimCountryDef.entityId }
  case object LOCATIONS   extends GroupHierarchical { override def mappedIdCol: String = BridgeVacancyLocationDef.entityId }

  case object LANGUAGES   extends GroupNonHierarchical { override def mappedIdCol: String = BridgeVacancyLanguageDef.entityId }
  case object LANGUAGES_LEVEL extends GroupNonHierarchical { override def mappedIdCol: String = BridgeVacancyLanguageDef.languageLevelId }

  val values: Set[NormalizerType] = Set(
    CURRENCY, EMPLOYER, EXPERIENCE, PLATFORM, EMPLOYMENTS,
    FIELDS, GRADES, SCHEDULES, SKILLS, COUNTRY, LOCATIONS, LANGUAGES,
    LANGUAGES_LEVEL
  )

  lazy val nonHierarchical: Set[GroupNonHierarchical] = values.collect { case x: GroupNonHierarchical => x }
  lazy val hierarchical: Set[GroupHierarchical] = values.collect { case x: GroupHierarchical => x }
}
