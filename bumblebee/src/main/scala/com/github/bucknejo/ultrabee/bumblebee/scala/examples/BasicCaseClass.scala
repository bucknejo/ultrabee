package com.github.bucknejo.ultrabee.bumblebee.scala.examples

case class BasicCaseClass(a: String, b: String)

object BasicCaseClassObject {

  def main(args: Array[String]): Unit = {
    basicCaseClasses.foreach(b => println(b))
  }

  val basicCaseClasses = List(
    BasicCaseClass("a", "d"),
    BasicCaseClass("b", "e"),
    BasicCaseClass("c", "f")
  )

  def compareBasicCaseClasses(a: BasicCaseClass, b: BasicCaseClass): Boolean = {
    a == b
  }


}
