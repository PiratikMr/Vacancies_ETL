package org.example.config.TableConfig

sealed trait UpdateStrategy


case object DoNothing extends UpdateStrategy

case class ExplicitUpdates(cols: Seq[String]) extends UpdateStrategy

case object UpdateAllExceptKeys extends UpdateStrategy