import sbtassembly.Plugin._
import sbtassembly.Plugin.MergeStrategy
import AssemblyKeys._

assemblySettings

name := "komono"

version := "1.0"

scalaVersion := "2.10.5"

mainClass in (Compile, run) := Some("org.komono.sqlanalytics.Main")

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

resolvers += Resolver.sonatypeRepo("public")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "com.typesafe" %% "scalalogging-slf4j" % "1.0.0",
  "org.slf4j" % "slf4j-api" % "1.7.1",
  "ch.qos.logback" % "logback-classic" % "1.0.7",
  "com.google.guava" % "guava" % "18.0",
  "com.facebook.presto" % "presto-parser" % "0.133",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.6.3",
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.7.0",
  "com.github.scopt" %% "scopt" % "3.3.0"
)

test in assembly := {}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", "MANIFEST.MF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}