package com.github.bucknejo.ultrabee.bumblebee.scala.examples

object NamedParameters {

  def main(args: Array[String]): Unit = {
    println(fullName(firstName = "Han", lastName = "Solo"))
    println(fullName(lastName = "Skywalker", firstName = "Luke"))
    //println(fullName(LastName = "Palpatine", ""))  // does not compile
  }

  def fullName(firstName: String, lastName: String): String = {
    s"$firstName $lastName"
  }

}
