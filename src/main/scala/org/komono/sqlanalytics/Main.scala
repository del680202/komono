package org.komono.sqlanalytics

import org.komono.sqlanalytics.render.YamlRender
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;


case class Config(createViewStatment:String = null)

object Main {
  def main(args: Array[String]) {
      
    val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger];
    root.setLevel(Level.INFO)
    
    val parser = new scopt.OptionParser[Config]("SQLAnalysizer") {
      opt[Unit]("debug") hidden() action { (_, c) =>
        root.setLevel(Level.DEBUG)
        c
      } text ("createViewStatment is an String property")
      opt[String]('s', "source") required() action { (x, c) =>
        c.copy(createViewStatment = x)
      } text ("createViewStatment is an String property")
      note("some notes.\n")
      help("help") text ("java -jar xxxx.jar [-s|--source] $createviewstatement")
    }
    // parser.parse returns Option[C]
    parser.parse(args, Config()) map { config =>
      val analyzer = SQlAnalyzerFactory.buildSQlAnalyzer("presto")
      val render = new YamlRender
      println(render.render(analyzer.analyzeCreateViewStatement(config.createViewStatment)))
    } getOrElse {
      // arguments are bad, usage message will have been displayed
    }
  }
}