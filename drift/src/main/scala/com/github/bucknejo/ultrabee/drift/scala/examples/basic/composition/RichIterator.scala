package com.github.bucknejo.ultrabee.drift.scala.examples.basic.composition

trait RichIterator extends AbstractIterator {

  def foreach(f: T => Unit): Unit = while (hasNext) f(next())

}

