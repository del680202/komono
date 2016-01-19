package org.komono.sqlanalytics


trait SQlAnalyzer {
  def analyzeCreateViewStatement(createViewStatment: String):CreateViewAnalysisResult
}