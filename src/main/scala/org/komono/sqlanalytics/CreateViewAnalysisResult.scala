package org.komono.sqlanalytics

case class CreateViewAnalysisResult(
  var targetName: Option[String] = None,
  var tables: Set[String] = Set.empty[String],
  var joinKeys: Set[String] = Set.empty[String],
  var whereKeys: Set[String] = Set.empty[String]){
  
  def mergeAnalysisResult(target:CreateViewAnalysisResult){
    this.tables = this.tables union target.tables
    this.joinKeys = this.joinKeys union target.joinKeys
    this.whereKeys = this.whereKeys union target.whereKeys
  }
  
  def addTable(table:String){
    this.tables = this.tables + table
  }
  
  def addJoinKeys(joinKeys:Set[String]){
    this.joinKeys = this.joinKeys union joinKeys
  }
  
    def addWhereKeys(whereKeys:Set[String]){
    this.whereKeys = this.whereKeys union whereKeys
  }
}