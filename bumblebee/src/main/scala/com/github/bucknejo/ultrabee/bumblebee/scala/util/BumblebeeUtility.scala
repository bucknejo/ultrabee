package com.github.bucknejo.ultrabee.bumblebee.scala.util

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.log4j.{LogManager, Logger}

import scala.util.{Failure, Success, Try}

trait BumblebeeUtility {

  @transient lazy val logger: Logger = LogManager.getLogger("Bumblebee")

  val properties: Properties = Try({
    val prop: Properties = new Properties()
    val path = "bumblebee.properties"
    prop.load(new FileInputStream(new File(getClass.getClassLoader.getResource(path).getFile)))
    prop
  }) match {
    case Success(s) => s
    case Failure(f) =>
      logger.error(s"[${this.getClass.getName}] Could not load properties for Bumblebee", f)
      throw f
  }

  val STATIC_DATA_PATH_LOCAL: Boolean = properties.getProperty("static.data.path.local").toBoolean
  val STATIC_DATA_PATH_MANUFACTURERS: String =
    staticDataPath(properties.getProperty("static.data.path.manufacturers"), STATIC_DATA_PATH_LOCAL)
  val STATIC_DATA_PATH_PRODUCTS: String =
    staticDataPath(properties.getProperty("static.data.path.products"), STATIC_DATA_PATH_LOCAL)

  def logBumblebeeException(bumblebee: BumblebeeException, className: String): Unit = {
    logger.error(s"[$className] ${BumblebeeConstants.BANNER_ERROR}")
    logger.error(s"[$className] Level: ${bumblebee.returnCode.level}")
    logger.error(s"[$className] Code: ${bumblebee.returnCode.code}")
    logger.error(s"[$className] Description: ${bumblebee.returnCode.description}")
    logger.error(s"[$className] Link: ${bumblebee.returnCode.link}")
    logger.error(s"[$className] ${BumblebeeConstants.BANNER_ERROR}")
  }

  def staticDataPath(path: String, isLocal: Boolean): String = {
    if (isLocal)
      new File(getClass.getClassLoader.getResource(path).getFile).getAbsolutePath
    else
      path
  }

}
