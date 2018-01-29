package com.github.bucknejo.ultrabee.drift.scala.examples.basic

object HigherOrderFunctions {

  def main(args: Array[String]): Unit = {
    println(listMapWrapper(things, doubleIt))
    val constructedFunction = functionBuilder(2, "test")
    val genFunctionRes = constructedFunction(5, "what")
    println(genFunctionRes)
  }

  val things: List[Double] = List(1,2,3,4,5)
  val doubleIt:(Double) => Double = (x: Double) => x * 2
  val newThings: List[Double] = things.map(doubleIt)

  val moreNewThings: List[Double] = things.map(x => x * 2)
  val evenMoreNewThings: List[Double] = things.map(_ * 2)

  def listMapWrapper(things: List[Double], mapFunction: (Double) => Double): List[Double] = {
    things.map(mapFunction)
  }

  def functionBuilder(num: Int, str: String): (Int, String) => String = {
    val twoNums = num * 2
    val twoStrs = str * 2
    (twoNumsPlus: Int, twoStrsPlus: String) => s"num: $num twoNums: $twoNums twoNumsPlus: $twoNumsPlus str: $str twoStrs: $twoStrs twoStrsPlus: $twoStrsPlus"
  }

}
