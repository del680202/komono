package org.komono.sqlanalytics.render

import org.komono.sqlanalytics.SQlAnalyzerFactory;
import org.scalatest.FunSuite

class TestYamlRender extends FunSuite {
  
   val testcase1 = "CREATE VIEW V1 AS SELECT pv FROM item3 JOIN item4 ON item3.id=item4.id WHERE b='b'"
  
  test("Test render to output yaml string") {
    val analyzer = SQlAnalyzerFactory.buildSQlAnalyzer("presto")
    val render = new YamlRender
    assertResult("""---
targetName: "v1"
tables:
- "item3"
- "item4"
joinKeys:
- "item3.id"
- "item4.id"
whereKeys:
- "b"
""")(render.render(analyzer.analyzeCreateViewStatement(testcase1)))
  }
}