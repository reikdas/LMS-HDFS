name := "lmshdfs"
version := "0.1"

scalaVersion := "2.12.10"

resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies += "org.scalatest" % "scalatest_2.12" % "3.0.8" % "test"

libraryDependencies += "org.scala-lang" % "scala-compiler" % scalaVersion.value % "compile"

libraryDependencies += "org.scala-lang" % "scala-library" % scalaVersion.value % "compile"

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value % "compile"

autoCompilerPlugins := true

val paradiseVersion = "2.1.0"

parallelExecution in Test := false

addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)

val lms_loc = "/home/reikdas/Research/lms-clean"
lazy val lms = ProjectRef(file(lms_loc), "lms-clean")
val lms_path = sys.props.getOrElseUpdate("LMS_PATH", lms_loc)

lazy val lmshdfs = (project in file(".")).dependsOn(lms % "compile->compile")
