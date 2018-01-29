package com.github.bucknejo.ultrabee.drift.scala.examples.collections

/**
  * from https://oldfashionedsoftware.com/2009/07/30/lots-and-lots-of-foldleft-examples/
  */

class FoldLefting {

  def sum01(list: List[Int]): Int = list.foldLeft(0)((r,c) => r+c)
  def sum02(list: List[Int]): Int = list.foldLeft(0)(_+_)
  def product(list: List[Int]): Int = list.foldLeft(1)(_*_)
  def count(list: List[Any]): Int = list.foldLeft(0)((sum,_) => sum + 1)
  def average01(list: List[Double]): Double = list.foldLeft(0.0)(_+_) / list.foldLeft(0.0)((r,_) => r+1)
  def average02(list: List[Double]): Double = list match {
    case head :: tail => tail.foldLeft( (head,1.0) )((r,c) =>
      ((r._1 + (c/r._2)) * r._2 / (r._2+1), r._2+1) )._1
    case Nil => Double.NaN
  }
  def last[A](list: List[A]): A = list.foldLeft[A](list.head)((_, c) => c)
  def penultimate[A](list: List[A]): A = list.foldLeft( (list.head, list.tail.head) )((r, c) => (r._2, c) )._1

}
