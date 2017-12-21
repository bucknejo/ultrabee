package com.github.bucknejo.ultrabee.bumblebee.scala.util

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

trait BumblebeeUserDefinedFunctions {

  def defineQuantity: UserDefinedFunction = udf((amt: Double) => if(amt>0) 1 else 0)

}
