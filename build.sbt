import Dependencies._

resolvers ++= Seq(
  "Hadoop Releases" at "https://repository.cloudera.com/content/repositories/releases/",
  "Cloudera Repo" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  Resolver.bintrayRepo("mziccard", "maven")

)

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "frauddetection",
      scalaVersion := "2.10.7",
      version      := "0.3"
    )),
    name := "Fraud Detection",
    libraryDependencies ++= Seq(
      "org.apache.spark" % "spark-streaming_2.10" % "1.6.0-cdh5.11.2",
      "org.apache.spark" % "spark-core_2.10" % "1.6.0-cdh5.11.2",
      "io.jvm.uuid" %% "scala-uuid" % "0.2.3"
    )
  )
