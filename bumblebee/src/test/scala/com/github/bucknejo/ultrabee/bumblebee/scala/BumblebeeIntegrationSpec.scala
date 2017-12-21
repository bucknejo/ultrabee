package com.github.bucknejo.ultrabee.bumblebee.scala

import cucumber.api.CucumberOptions
import cucumber.api.junit.Cucumber
import org.junit.runner.RunWith

@RunWith(classOf[Cucumber])
@CucumberOptions(
  features = Array("classpath:features"),
  tags = Array("not @ignore"),
  glue = Array("com.github.bucknejo.ultrabee.bumblebee.scala"),
  plugin = Array("pretty", "html:target/cucumber/html", "json:target/cucumber/json")
)
class BumblebeeIntegrationSpec
