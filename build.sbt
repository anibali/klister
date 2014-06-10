name := "klister"

version := "0.0.0"

scalacOptions ++= Seq("-feature")

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.3.0-cdh5.0.0"

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

packSettings

packMain := Map("klister" -> "org.nibali.klister.Main")
