name := "tcep"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.12.1"
val akkaVersion = "2.6.8" // 2.6.8, newer version causes problem with AkkaHttp;
val akkaHttpVersion = "10.2.4"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
  "com.typesafe.akka" % "akka-cluster-metrics_2.12" % akkaVersion,
  "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion,
  "com.typesafe.akka" %% "akka-http"   % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion, // needed by akka-http
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "io.netty" % "netty" % "3.10.6.Final", // for using classic akka remoting instead of artery
  "ch.megard" %% "akka-http-cors" % "0.3.1",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.espertech"     %  "esper"        % "5.5.0",
  "com.twitter" % "chill-akka_2.12" % "0.9.5",
  "org.scala-lang" % "scala-reflect" % "2.12.1",
  "org.scalaj" %% "scalaj-http" % "2.4.1",
  "commons-lang" % "commons-lang" % "2.6",
  "com.google.guava" % "guava" % "19.0",
  "org.scalanlp" %% "breeze" % "1.0",           //Added for Linear Algebra in Benedikt's thesis
  //"org.scalanlp" %% "breeze-natives" % "1.0",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion  % "test",
  "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % "test",
  "org.scalatest"     %% "scalatest"    % "3.0.1"   % "test",
  "org.mockito"       % "mockito-core"  % "2.23.0"  % "test"
)

// coala-related
// temporary workaround for dependency missing from default repository
resolvers += "XypronRelease" at "https://www.xypron.de/repository"
libraryDependencies += "org.gnu.glpk" % "glpk-java" % "1.11.0"
resolvers += "CardyGAn" at "https://github.com/Echtzeitsysteme/cardygan-mvn/raw/master"
libraryDependencies += "org.coala" % "coala-core" % "0.0.5-SNAPSHOT"
//resolvers += "JenaBio" at "https://bio.informatik.uni-jena.de/repository/libs-release-oss/"
//libraryDependencies += "cplex" % "cplex" % "12.8"
//dependencyOverrides += "org.cardygan" % "ilp" % "0.1.12" // force correct cardygan ilp version
// force older version because org.eclipse.xtext v2.12.0 tries to use the field EOF_TOKEN from org.antlr.runtime, which is not present in newest version
dependencyOverrides += "org.antlr" % "antlr-runtime" % "3.2"
// explicitly add most recent version to avoid assembly merge conflicts due to strange transitive dependency structure of emf ecore and common
libraryDependencies += "org.eclipse.emf" % "org.eclipse.emf.ecore" % "2.24.0"
libraryDependencies += "org.eclipse.emf" % "org.eclipse.emf.common" % "2.20.0"

import com.typesafe.sbt.SbtMultiJvm.multiJvmSettings

lazy val root = (project in file("."))
  .enablePlugins(MultiJvmPlugin) // use the plugin
  .configs(MultiJvm) // load the multi-jvm configuration
  .settings(multiJvmSettings: _*) // apply the default settings
  .settings(
    parallelExecution in Test := false // do not run test cases in parallel
  )

assemblyMergeStrategy in assembly := {
  case "module-info.class" => MergeStrategy.discard
  case PathList(xs @ _*) if xs.last == "IlpBasedPlanner.class" => MergeStrategy.first
  case "org/coala/planner/IlpBasedPlanner.class" => MergeStrategy.first
  case e if e.endsWith(".exsd") => MergeStrategy.last
  case ".api_description" => MergeStrategy.filterDistinctLines
  case "plugin.xml"       => MergeStrategy.last
  case "plugin.properties"=> MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

//mainClass in(Compile, run) := Some("tcep.simulation.tcep.SimulationRunner")
mainClass in assembly := Some("tcep.simulation.tcep.SimulationRunner")
assemblyJarName in assembly := "tcep_2.12-0.0.1-SNAPSHOT-one-jar.jar"
test in assembly := {} // skip tests on build


 