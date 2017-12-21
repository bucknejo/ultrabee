package com.github.bucknejo.ultrabee.bumblebee.scala.util

case class BumblebeeException(returnCode: BumblebeeReturnCode, cause: Throwable) extends Exception(cause)
