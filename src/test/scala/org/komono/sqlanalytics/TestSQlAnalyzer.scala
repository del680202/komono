

package org.komono.sqlanalytics

import collection.mutable.Stack
import org.scalatest.FunSuite

class TestSQlAnalyzer extends FunSuite {

  val testcase1 = "CREATE VIEW V1 AS SELECT pv FROM item"
  val testcase2 = "CREATE VIEW V2 AS SELECT pv FROM item2 WHERE a='b'"
  val testcase3 = "CREATE VIEW V3 AS SELECT pv FROM item3 JOIN item4 ON item3.id=item4.id WHERE b='b'"
  val testcase4 = """create or replace view multijoin.test
as
select
    *
from
(
select
    *
from
        (select * from table1 where dt=date_format(date_add('day', -1, now()),'%Y%m%d')) a
inner join
        (select * from table2 where dt=date_format(date_add('day', -1, now()),'%Y%m%d')) b
on
        a.copy_id=b.copy_id
inner join
        (select * from table3 where dt=date_format(date_add('day', -1, now()),'%Y%m%d')) c
on
        b.seq_test_id=c.seq_test_id
) tmp"""

  val testcase5 = """CREATE OR REPLACE VIEW multijoin.test2
AS
SELECT
  *
FROM
  (SELECT
    *
  FROM
    hive.table1) weekly_data
JOIN
  (SELECT
    *
  FROM
    hive.table2 D
  LEFT JOIN
    hive.table3 T
  ON
    D.dt=T.yyyymmdd
  GROUP BY
    D.id1, D.id2, T.id3) daily_summary_data
ON
  weekly_data.id1=daily_summary_data.id1 AND weekly_data.key1=daily_summary_data.key1
LEFT JOIN
  hive.table4 oa_stats2
ON
  oa_stats2.dt=weekly_data.yyyymmdd AND cast(oa_stats2.id1 as varchar) =daily_summary_data.id1
LEFT JOIN
  (SELECT * FROM hive.table5) meta_data
ON
  meta_data.id2=weekly_data.id2"""

  val testcase6 = """create or replace view multijoin.test3
AS
select
  *
from
(
  select
    *
  from
  (
    select
      *
    from
    (
      select
        *
      from
        hive.table1
      where
        message_flag=1 and via='app'
    )tmp
    group by dt
  )message_account
  left outer join
  (
    select
      *
    from
    (
      select
        *
      from
        hive.table2
      where
        post_flag=1 and via='app'
    )tmp
    group by dt
  )post_account
  on
    message_account.dt = post_account.dt
  left outer join
  (
    select
      *
    from
    (
      select
        *
      from
        hive.table3
      where
        message_or_post_flag=1 and via='app'
    )tmp
    group by dt
  )message_or_post_account
  on
    message_account.dt = message_or_post_account.dt
)a
left outer join
  hive.table4 b
on
  a.dt = b.yyyymmdd
where
  cast(a.dt as bigint) >= 20150201
"""

  val testcase7 = """create view union_test1
as
select
        *
from
        hive.table1
group by
        dt
union all
select
        *
from
         hive.table2
group by
        dt"""
  
  val testcase8 = """create view union_test2
as
SELECT A.* FROM (
select
        *
from
        hive.table1
group by
        dt
union all
select
        *
from
         hive.table2
group by
        dt) A"""
  
  val testcase9 = """CREATE OR REPLACE VIEW union_test3
AS
SELECT
    *
FROM
        (SELECT
          *
        FROM
                (SELECT
                  *
                FROM
                  (SELECT * FROM hive.table1 WHERE is_limit = false) a
                CROSS JOIN
                  hive.table2 b
                ) dm
        WHERE
                dm.range between dm.min AND dm.max
        UNION ALL
        SELECT
                *
        FROM
                (SELECT * FROM hive.table3 WHERE is_limit = true) a
         )tmp
UNION ALL
SELECT * FROM
    (SELECT DISTINCT d_type FROM hive.table4)
CROSS JOIN
    (SELECT 0 as d1, '0' as d2, '0' as key) a"""

  test("Test analyzing result of create statement") {
    val analyzer = SQlAnalyzerFactory.buildSQlAnalyzer("presto")
    var result = analyzer.analyzeCreateViewStatement(testcase1)
    assertResult("v1")(result.targetName.get)
    assertResult(Set("item"))(result.tables)
    assertResult(Set())(result.joinKeys)
    assertResult(Set())(result.whereKeys)

    result = analyzer.analyzeCreateViewStatement(testcase2)
    assertResult("v2")(result.targetName.get)
    assertResult(Set("item2"))(result.tables)
    assertResult(Set())(result.joinKeys)
    assertResult(Set("a"))(result.whereKeys)

    result = analyzer.analyzeCreateViewStatement(testcase3)
    assertResult("v3")(result.targetName.get)
    assertResult(Set("item3", "item4"))(result.tables)
    assertResult(Set("item3.id", "item4.id"))(result.joinKeys)
    assertResult(Set("b"))(result.whereKeys)

    result = analyzer.analyzeCreateViewStatement(testcase4)
    assertResult("multijoin.test")(result.targetName.get)
    assertResult(Set("table1", "table2", "table3"))(result.tables)
    assertResult(Set("a.copy_id", "b.copy_id", "b.seq_test_id", "c.seq_test_id"))(result.joinKeys)
    assertResult(Set("dt"))(result.whereKeys)

    result = analyzer.analyzeCreateViewStatement(testcase5)
    assertResult("multijoin.test2")(result.targetName.get)
    assertResult(Set("hive.table1",
      "hive.table2",
      "hive.table3",
      "hive.table4",
      "hive.table5"))(result.tables)
    assertResult(Set("daily_summary_data.id1",
      "t.yyyymmdd",
      "weekly_data.yyyymmdd",
      "meta_data.id2",
      "daily_summary_data.key1",
      "oa_stats2.dt",
      "d.dt",
      "oa_stats2.id1",
      "weekly_data.id2",
      "weekly_data.id1",
      "weekly_data.key1"))(result.joinKeys)
    assertResult(Set())(result.whereKeys)

    result = analyzer.analyzeCreateViewStatement(testcase5)

    result = analyzer.analyzeCreateViewStatement(testcase6)
    assertResult("multijoin.test3")(result.targetName.get)
    assertResult(Set("hive.table1",
      "hive.table2",
      "hive.table3",
      "hive.table4"))(result.tables)
    assertResult(Set("a.dt",
      "b.yyyymmdd",
      "message_account.dt",
      "post_account.dt",
      "message_or_post_account.dt"))(result.joinKeys)
    assertResult(Set("message_or_post_flag",
      "via",
      "post_flag",
      "message_flag",
      "a.dt"))(result.whereKeys)
      
    result = analyzer.analyzeCreateViewStatement(testcase7)
    assertResult("union_test1")(result.targetName.get)
    assertResult(Set("hive.table1", "hive.table2"))(result.tables)
    assertResult(Set())(result.joinKeys)
    assertResult(Set())(result.whereKeys)
    
    result = analyzer.analyzeCreateViewStatement(testcase8)
    assertResult("union_test2")(result.targetName.get)
    assertResult(Set("hive.table1", "hive.table2"))(result.tables)
    assertResult(Set())(result.joinKeys)
    assertResult(Set())(result.whereKeys)
    
    result = analyzer.analyzeCreateViewStatement(testcase9)
    assertResult("union_test3")(result.targetName.get)
    assertResult(Set("hive.table1", "hive.table2", "hive.table3", "hive.table4"))(result.tables)
    assertResult(Set())(result.joinKeys)
    assertResult(Set("is_limit", "dm.range", "dm.min", "dm.max"))(result.whereKeys)
  }
}
