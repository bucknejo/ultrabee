package com.github.bucknejo.ultrabee.drift.scala.examples.basic

object Methods {

  def main(args: Array[String]): Unit = {
    println(concat("Death", "Star"))
    println(multiParamList(1,2)(3))
    val curry: (Int) => Int = multiParamList(9,10)
    println(s"curry: ${curry(3)}")
  }

  def concat(x: String, y: String): String = {
    s"$x $y"
  }

  def multiParamList(x: Int, y: Int)(z: Int): Int = {
    (x + y) / z
  }



}
