package com.github.bucknejo.ultrabee.bumblebee.scala.examples

trait RichIterator extends AbstractIterator {

  def foreach(f: T => Unit): Unit = while (hasNext) f(next())

}

