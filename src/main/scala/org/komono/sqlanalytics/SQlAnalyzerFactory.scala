package org.komono.sqlanalytics


object SQlAnalyzerFactory {
  def buildSQlAnalyzer(name: String) : SQlAnalyzer = {
    val analyzer:SQlAnalyzer = name match {
      case "presto" => new PrestoSQLAnalyzer()
      case _ => null
    }
    if(analyzer == null) throw new Exception("Unknown analyzer name")
    analyzer
  }
}