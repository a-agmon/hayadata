ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "IcebergSpark"
  )
val sparkVersion = "3.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  //iceberg

  //iceberg - full path is iceberg-spark-runtime-3.2_2.12
  "org.apache.iceberg" %% "iceberg-spark-runtime-3.3" % "1.2.1",
  "software.amazon.awssdk" % "url-connection-client" % "2.20.26",
  "software.amazon.awssdk" % "bundle" % "2.20.26",

  "org.scala-lang.modules" %% "scala-collection-compat" % "2.8.1",

)
// for sbt assembly
// merge strategy for build with assembly
assembly / assemblyMergeStrategy   := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
