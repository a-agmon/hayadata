ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "IcebergScalaClient",
    idePackagePrefix := Some("com.aagmon.icebergtalk")
  )

libraryDependencies ++= Seq (

  // for the client
  "org.apache.iceberg" % "iceberg-core" % "1.2.0",
  "org.apache.iceberg" % "iceberg-aws" % "1.2.0",
  "org.apache.iceberg" % "iceberg-data" % "1.2.0",
  "org.apache.iceberg" % "iceberg-parquet" % "1.2.0",

  "software.amazon.awssdk" % "bundle" % "2.20.26",
  "software.amazon.awssdk" % "url-connection-client" % "2.20.26",
  "org.apache.hadoop" % "hadoop-client" % "3.3.2",
  "org.scala-lang.modules" %% "scala-collection-compat" % "2.8.1"


)


