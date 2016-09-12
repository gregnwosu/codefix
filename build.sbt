name := "Transactions"

version := "1.0"

scalaVersion := "2.11.8"

 libraryDependencies ++= Seq(
   "org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
   "org.apache.spark" %% "spark-mllib" % "2.0.0" % "provided",
   "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided")

mainClass in (Compile, run) := Some("Main")

assemblyJarName in assembly := "dataset-example.jar"

mainClass in assembly := Some("Main")
