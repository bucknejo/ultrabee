package com.github.bucknejo.ultrabee.bumblebee.scala.examples

class BasicClass(a: String, b: String) {

  def concat(delimiter: String): String = {
    s"$a$delimiter$b"
  }

}
