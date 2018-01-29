package com.github.bucknejo.ultrabee.drift.scala.examples.basic

object AnonymousFunctions {

  def main(args: Array[String]): Unit = {
    println(noParams()) // 3
    println(oneParam(1)) // 2
    println(twoParams(3, 4)) // 7
  }

  val noParams: () => Int = () => 1 + 2
  val oneParam: (Int) => Int = (x: Int) => x + 1
  val twoParams: (Int, Int) => Int = (x: Int, y: Int ) => x + y

}
