name := "lmshdfs"
version := "0.1"

scalaVersion := "2.12.10"

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies += "org.scalatest" % "scalatest_2.12" % "3.0.4"

libraryDependencies += "org.scala-lang" % "scala-compiler" % scalaVersion.value % "compile"

libraryDependencies += "org.scala-lang" % "scala-library" % scalaVersion.value % "compile"

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value % "compile"

autoCompilerPlugins := true

val paradiseVersion = "2.1.0"

addCompilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full)

lazy val lms = ProjectRef(file("/home/reikdas/Research/lms-clean"), "lms-clean")
lazy val flare = ProjectRef(file("/home/reikdas/Research/flare"), "flare")

lazy val lmshdfs = (project in file(".")).dependsOn(lms % "compile->compile", flare % "compile->compile")
