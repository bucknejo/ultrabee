package com.github.bucknejo.ultrabee.core.scala

import com.github.bucknejo.ultrabee.api.scala.Ingestible
import com.github.bucknejo.ultrabee.bumblebee.scala.Bumblebee
import com.github.bucknejo.ultrabee.bumblebee.scala.service.{BumblebeeExtractor, BumblebeeLoader, BumblebeeTransformer}

object IngestionFactory {

  def apply(source: String): Option[Ingestible] = {

      source match {
        case "BMB" =>
          val extractor = new BumblebeeExtractor()
          val transformer = new BumblebeeTransformer()
          val loader = new BumblebeeLoader()
          Some(new Bumblebee(extractor, transformer, loader))
        case _ => None
      }

  }

}
