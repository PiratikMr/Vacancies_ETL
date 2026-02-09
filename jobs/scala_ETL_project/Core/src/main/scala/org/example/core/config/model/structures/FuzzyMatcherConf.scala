package org.example.core.config.model.structures

import org.example.core.objects.NormalizersEnum.NormalizerType

case class FuzzyMatcherConf(
                             private val configs: Map[NormalizerType, FuzzyMatchSettings]
                           ) {

  def get(normalizerType: NormalizerType): FuzzyMatchSettings = {
    configs(normalizerType)
  }

}
