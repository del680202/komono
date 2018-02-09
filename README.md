# komono

A small tool for parsing DDL of Presto, what tables/joinKeys/whereKeys exist in statement
<br/>
It output result by YAML

#### Input

```sql
CREATE VIEW view1
AS
SELECT
  A.dt
  B.pv
FROM
  table1 A
JOIN
  table2 B
ON
  A.uid=B.uid
WHERE
  A.dt='20160101'
```

#### Output

```yaml
---
targetName: "view1"
tables:
- "table1"
- "table2"
joinKeys:
- "a.uid"
- "b.uid"
whereKeys:
- "a.dt"

```

# Requirements

* Mac OS X or Linux
* Scala 2.10.5
* Java 1.8+
* sbt (for building)

sbt http://www.scala-sbt.org/
<br/>
Scala IDE http://scala-ide.org/

# Build

```
$ git clone https://github.com/del680202/komono.git
$ cd komono
$ sbt clean assembly
```

# Run

```
$ java -jar target/scala-2.10/komono-assembly-1.0.jar --source "CREATE VIEW view1 AS SELECT A.dt,B.pv FROM table1 A JOIN table2 B ON A.uid=B.uid WHERE A.dt=20160101"
```

# Make executable file

```
$ sudo cp target/scala-2.10/komono-assembly-1.0.jar /usr/share/komono-assembly-1.0.jar
$ sudo vim /usr/bin/komono

//paste context as below
#!/bin/bash
java -jar /usr/share/komono-assembly-1.0.jar "$@"

$ sudo chmod +x /usr/bin/komono
$ komono --source "CREATE VIEW view1 AS SELECT A.dt,B.pv FROM table1 A JOIN table2 B ON A.uid=B.uid WHERE A.dt=20160101"
```
