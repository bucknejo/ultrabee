package com.github.bucknejo.ultrabee.bumblebee.scala

import org.apache.log4j.Logger
import org.apache.log4j.LogManager

trait BumblebeeHelper {

  @transient lazy val logger: Logger = LogManager.getLogger("Bumblebee")

}
