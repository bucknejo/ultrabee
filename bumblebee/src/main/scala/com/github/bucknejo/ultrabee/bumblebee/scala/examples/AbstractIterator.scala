package com.github.bucknejo.ultrabee.bumblebee.scala.examples

abstract class AbstractIterator {
  type T
  def hasNext: Boolean
  def next(): T
}
