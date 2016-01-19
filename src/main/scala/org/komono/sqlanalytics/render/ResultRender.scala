package org.komono.sqlanalytics.render

import org.komono.sqlanalytics.CreateViewAnalysisResult

/*
 * Output analysisResult for any format
 * 
 * */
trait ResultRender {
  type T
  def render(result:CreateViewAnalysisResult):T
}