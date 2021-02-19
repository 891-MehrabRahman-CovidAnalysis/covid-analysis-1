name := "covid-econ-grp"
ThisBuild / version := "1.0"
ThisBuild / organization := "com.revatureData.econGroup"
ThisBuild / scalaVersion := "2.12.13"

lazy val global = project
  .in(file("."))
  .settings(settings)
  .disablePlugins(AssemblyPlugin)
  .aggregate(
    Utilities,
    CovidResponse,
    CorrelateInfectionGDP,
    FirstRegionalPeaks,
    CountryBorders
  )

lazy val Utilities = project
  .settings(
    name := "Utilities",
    assemblyJarName in assembly := name.value + ".jar",
    settings,
    libraryDependencies ++= sharedDependencies ++ Seq(
      dependencies.aws
    )
  )
  .disablePlugins(AssemblyPlugin)

lazy val CovidResponse = project
  .settings(
    name := "CovidResponse",
    assemblyJarName in assembly := name.value + ".jar",
    settings,
    assemblySettings,
    libraryDependencies ++= sharedDependencies ++ Seq(
      dependencies.logCore,
      dependencies.logScala
    )
  )
  .dependsOn(Utilities)

lazy val CorrelateInfectionGDP = project
  .settings(
    name := "CorrelateInfectionGDP",
    assemblyJarName in assembly := name.value + ".jar",
    settings,
    assemblySettings,
    libraryDependencies ++= sharedDependencies ++ Seq(
      dependencies.logCore,
      dependencies.logScala
    )
  )
  .dependsOn(Utilities)

lazy val FirstRegionalPeaks = project
  .settings(
    name := "FirstRegionalPeaks",
    assemblyJarName in assembly := name.value + ".jar",
    settings,
    assemblySettings,
    libraryDependencies ++= sharedDependencies ++ Seq(
      dependencies.logCore,
      dependencies.logScala
    )
  )
  .dependsOn(Utilities)

lazy val CountryBorders = project
  .settings(
    name := "CountryBorders",
    assemblyJarName in assembly := name.value + ".jar",
    settings,
    assemblySettings,
    libraryDependencies ++= sharedDependencies ++ Seq(
      dependencies.logCore,
      dependencies.logScala
    )
  )
  .dependsOn(Utilities)

// Dependencies
lazy val sharedDependencies = Seq(
  dependencies.sparkCore,
  dependencies.sparkSql,
  dependencies.scalaTest,
  dependencies.mrPowers
)

lazy val dependencies =
  new {
    val aws = "com.amazonaws" % "aws-java-sdk" % "1.3.32"
    val sparkCore = "org.apache.spark" %% "spark-core" % "2.4.5"
    val sparkSql = "org.apache.spark" %% "spark-sql" % "2.4.5"
    val scalaTest = "org.scalatest" %% "scalatest-funspec" % "3.2.2" % "test"
    val mrPowers = "com.github.mrpowers" %% "spark-fast-tests" % "0.21.3" % "test"
    val logCore = "org.apache.logging.log4j" % "log4j-core" % "2.13.0" % Runtime
    val logScala = "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0"
  }

//Settings
lazy val settings =
  commonSettings ++
    wartremoverSettings ++
    scalafmtSettings

lazy val commonSettings = Seq(
  scalacOptions ++= compilerOptions,
  resolvers ++= Seq(
    "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  )
)

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := name.value + ".jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", _*) => MergeStrategy.discard
    case "application.conf"       => MergeStrategy.concat
    case _                        => MergeStrategy.first
  }
)

lazy val wartremoverSettings = Seq(
  wartremoverWarnings in (Compile, compile) ++= Warts.allBut(Wart.Throw)
)

lazy val scalafmtSettings =
  Seq(
    scalafmtOnCompile := true,
    scalafmtTestOnCompile := true,
    scalafmtVersion := "1.2.0"
  )

lazy val compilerOptions = Seq(
  "-unchecked",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-deprecation",
  "-encoding",
  "utf8"
)
