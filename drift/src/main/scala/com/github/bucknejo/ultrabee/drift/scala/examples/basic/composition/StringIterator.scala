package com.github.bucknejo.ultrabee.drift.scala.examples.basic.composition

class StringIterator(s: String) extends AbstractIterator {

  type T = Char
  private var i = 0
  def hasNext: Boolean = i < s.length
  def next(): T = {
    val ch = s.charAt(i)
    i += 1
    ch
  }

}

object StringIterator extends App {

  class RichStringIterator extends StringIterator("Hoth") with RichIterator
  val richStringIterator = new RichStringIterator
  richStringIterator.foreach(println)

}
