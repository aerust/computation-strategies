organization := "com.socrata"

name := "computation-strategies"

scalaVersion := "2.12.8"

crossScalaVersions := Seq("2.10.4", "2.11.8", scalaVersion.value)

scalacOptions ++= Seq("-deprecation", "-feature")

resolvers += "socrata" at "https://repo.socrata.com/artifactory/libs-release"

libraryDependencies ++= Seq(
  "com.rojoma" %% "rojoma-json-v3" % "3.10.1",
  "com.socrata" %% "soql-types" % "2.11.21",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test"
)
