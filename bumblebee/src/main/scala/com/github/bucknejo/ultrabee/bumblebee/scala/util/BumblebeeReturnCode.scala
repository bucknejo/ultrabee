package com.github.bucknejo.ultrabee.bumblebee.scala.util

sealed abstract class BumblebeeReturnCode(val level: String, val code: Int, val description: String, val link: String)

case object ULTRABEE_ERROR_1 extends BumblebeeReturnCode("", 0, "", "")
case object ULTRABEE_ERROR_2 extends BumblebeeReturnCode("", 0, "", "")
