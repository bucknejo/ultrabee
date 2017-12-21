package com.github.bucknejo.ultrabee.core.scala

import org.apache.commons.cli.{CommandLine, Options, PosixParser}

import scala.util.{Failure, Success, Try}

trait IngestionUtility {

  def commandLine(args: Array[String]): Option[CommandLine] = {

    Try({
      val posixParser: PosixParser = new PosixParser
      val options: Options = new Options
      options.addOption("", "", true, "")
      posixParser.parse(options, args, false)
    }) match {
      case Success(s) => Some(s)
      case Failure(f) =>
        None
    }

  }

}
