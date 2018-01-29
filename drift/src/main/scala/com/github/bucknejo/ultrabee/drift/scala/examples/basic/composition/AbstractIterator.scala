package com.github.bucknejo.ultrabee.drift.scala.examples.basic.composition

abstract class AbstractIterator {
  type T
  def hasNext: Boolean
  def next(): T
}
