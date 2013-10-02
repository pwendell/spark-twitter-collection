name := "twitter-collector"

scalaVersion := "2.9.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "0.8.0-incubating",
  "org.twitter4j" % "twitter4j-core" % "3.0.3")

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Twitter4j" at "http://twitter4j.org/maven2/",
  "Spark RC5" at "https://repository.apache.org/content/repositories/orgapachespark-051/",
  "Spray Repository" at "http://repo.spray.cc/")
