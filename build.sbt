val rojomaJsonV3 = "com.rojoma" %% "rojoma-json-v3" % "3.9.1"
val soqlTypes = "com.socrata" %% "soql-types" % "2.11.3"

organization := "com.socrata"
name := "computation-strategies"
scalaVersion := "2.11.8"
crossScalaVersions := Seq("2.10.4", scalaVersion.value)
libraryDependencies ++= Seq(
  rojomaJsonV3,
  soqlTypes
)
com.socrata.sbtplugins.StylePlugin.StyleKeys.styleCheck in Test := {}
com.socrata.sbtplugins.StylePlugin.StyleKeys.styleCheck in Compile := {}
com.socrata.sbtplugins.findbugs.JavaFindBugsPlugin.JavaFindBugsKeys.findbugsFailOnError in Test := false
com.socrata.sbtplugins.findbugs.JavaFindBugsPlugin.JavaFindBugsKeys.findbugsFailOnError in Compile := false
