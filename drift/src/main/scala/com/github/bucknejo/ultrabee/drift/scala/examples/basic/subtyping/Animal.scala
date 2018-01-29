package com.github.bucknejo.ultrabee.drift.scala.examples.basic.subtyping

import scala.collection.mutable.ArrayBuffer

object Animal extends App {

  val dog = new Dog("Bob")
  val cat = new Cat("Jerry")
  val animals = ArrayBuffer.empty[Pet]
  animals.append(dog)
  animals.append(cat)
  animals.foreach(pet => println(pet.name))

}
