package org.komono.sqlanalytics

import com.facebook.presto.sql.parser.SqlParser
import com.facebook.presto.sql.tree.CreateView
import com.facebook.presto.sql.tree.QuerySpecification
import com.facebook.presto.sql.tree.Relation
import com.facebook.presto.sql.tree.AliasedRelation
import com.facebook.presto.sql.TreePrinter
import com.facebook.presto.sql.tree.DefaultTraversalVisitor
import com.facebook.presto.sql.tree.TableSubquery
import com.facebook.presto.sql.tree.Join
import com.google.common.base.Optional
import com.facebook.presto.sql.tree.Table
import com.facebook.presto.sql.tree.Query
import com.facebook.presto.sql.tree.JoinCriteria
import com.facebook.presto.sql.tree.JoinOn
import com.facebook.presto.sql.tree.ComparisonExpression
import com.facebook.presto.sql.tree.LogicalBinaryExpression
import com.facebook.presto.sql.tree.SingleColumn
import com.facebook.presto.sql.tree.Expression
import com.facebook.presto.sql.tree.DereferenceExpression
import com.facebook.presto.sql.tree.Cast
import com.facebook.presto.sql.tree.QualifiedNameReference
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory
import com.facebook.presto.sql.tree.Union

class PrestoSQLAnalyzer extends SQlAnalyzer {

  val logger = Logger(LoggerFactory.getLogger(this.getClass.getSimpleName))

  override def analyzeCreateViewStatement(createViewStatment: String): CreateViewAnalysisResult = {
    val parser = new SqlParser()
    val createStatement: CreateView = parser.createStatement(createViewStatment).asInstanceOf[CreateView]
    var result: CreateViewAnalysisResult = new CreateViewAnalysisResult(targetName = Some(createStatement.getName.toString))
    val query = createStatement.getQuery
    query.getQueryBody.getClass.getSimpleName match {
      case "QuerySpecification" => {
        handleQuerySpecification(result, query.getQueryBody)
      }
      case "Union" => {
        val union = query.getQueryBody.asInstanceOf[Union]
        for (relation <- union.getRelations.toArray()) {
          result.mergeAnalysisResult(buildAnalysisResult(relation.asInstanceOf[Relation]))
        }
      }
    }
    result
  }

  def handleQuerySpecification(result: CreateViewAnalysisResult, queryBodyRelation: Relation) {
    val queryBody = queryBodyRelation.asInstanceOf[QuerySpecification]
    if (queryBody.getFrom().isPresent()) {
      result.mergeAnalysisResult(buildAnalysisResult(queryBody.getFrom.get))
    }
    if (queryBody.getWhere.isPresent()) {
      result.addWhereKeys(parseExpression(queryBody.getWhere.get))
    }
  }

  def buildAnalysisResult(relation: Relation): CreateViewAnalysisResult = {
    var result = new CreateViewAnalysisResult
    relation.getClass.getSimpleName match {
      case "AliasedRelation" => {
        val aliasFrom = relation.asInstanceOf[AliasedRelation]
        result.mergeAnalysisResult(buildAnalysisResult(aliasFrom.getRelation))
      }
      case "TableSubquery" => {
        val query = relation.asInstanceOf[TableSubquery].getQuery
        query.getQueryBody.getClass.getSimpleName match {
          case "QuerySpecification" => {
            val queryBody = query.getQueryBody.asInstanceOf[QuerySpecification]
            if (queryBody.getFrom.get.isInstanceOf[Table]) {
              val table = queryBody.getFrom.get.asInstanceOf[Table]
              result.addTable(table.getName.toString())
            } else {
              result.mergeAnalysisResult(buildAnalysisResult(queryBody.getFrom.get))
            }
            val where = queryBody.getWhere
            if (where.isPresent()) {
              result.addWhereKeys(parseExpression(where.get))
            }
          }
          case "Union" => {
            val union = query.getQueryBody.asInstanceOf[Union]
            for (relation <- union.getRelations.toArray()) {
              result.mergeAnalysisResult(buildAnalysisResult(relation.asInstanceOf[Relation]))
            }
          }
        }
      }
      case "Join" => {
        val join = relation.asInstanceOf[Join]
        if (join.getCriteria.isPresent()) {
          result.addJoinKeys(parseExpression(join.getCriteria.get.asInstanceOf[JoinOn].getExpression))
        }
        result.mergeAnalysisResult(buildAnalysisResult(join.getLeft))
        result.mergeAnalysisResult(buildAnalysisResult(join.getRight))
      }
      case "Table" => {
        val table = relation.asInstanceOf[Table]
        result.addTable(table.getName.toString())
      }
      case "Union" => {
        println("QQ")
      }
      case "QuerySpecification" => {
        handleQuerySpecification(result, relation)
      }
      case _ => {
        logger.debug(s"Unknown relation: ${relation.getClass.getSimpleName}")
      }
    }
    result
  }

  /*
   * Build list of columns which are used in expression
   * */
  def parseExpression(expression: Expression): Set[String] = {

    var result: Set[String] = Set.empty[String]
    if (expression.isInstanceOf[ComparisonExpression]) {
      val op = expression.asInstanceOf[ComparisonExpression]
      result = result union parseExpression(op.getLeft)
      result = result union parseExpression(op.getRight)
    } else if (expression.isInstanceOf[LogicalBinaryExpression]) {
      val op = expression.asInstanceOf[LogicalBinaryExpression]
      result = result union parseExpression(op.getLeft)
      result = result union parseExpression(op.getRight)
    } else if (expression.isInstanceOf[DereferenceExpression]) {
      val op = expression.asInstanceOf[DereferenceExpression]
      result = result union Set(s"${op.getBase.toString()}.${op.getFieldName.toString()}".replaceAll("\"", ""))
    } else if (expression.isInstanceOf[Cast]) {
      val op = expression.asInstanceOf[Cast]
      result = result union parseExpression(op.getExpression)
    } else if (expression.isInstanceOf[QualifiedNameReference]) {
      val op = expression.asInstanceOf[QualifiedNameReference]
      val name: String = if (op.getName == op.getSuffix)
        op.getName.toString().replaceAll("\"", "")
      else
        s"${op.getName.toString()}.${op.getSuffix.toString()}".replaceAll("\"", "")
      result = result union Set(name)
    } else {
      logger.debug(s"unknown expression=$expression, className=${expression.getClass.getSimpleName}")
    }
    result

  }

}