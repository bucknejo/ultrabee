package com.github.bucknejo.ultrabee.drift.scala.examples.basic

class BasicClass(a: String, b: String) {

  def concat(delimiter: String): String = {
    s"$a$delimiter$b"
  }

}
