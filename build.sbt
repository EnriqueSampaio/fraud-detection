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
      scalaVersion := "2.10.6",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "Fraud Detection",
    libraryDependencies ++= Seq(
      "org.apache.spark" % "spark-streaming_2.10" % "1.6.0-cdh5.8.3",
      "org.apache.spark" % "spark-core_2.10" % "1.6.0-cdh5.8.3"
    )
  )
