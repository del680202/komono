package org.komono.sqlanalytics.render

import scala.collection.JavaConversions._
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.komono.sqlanalytics.CreateViewAnalysisResult


class YamlRender extends ResultRender{
  
  type T = String
  def render(result:CreateViewAnalysisResult):T = {
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    mapper.writeValueAsString(result)
  }
  
}

