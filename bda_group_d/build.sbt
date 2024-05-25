val scala3Version = "3.4.1"

lazy val root = project
  .in(file("."))
  .settings(
    name := "bda_group_d",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    // Add multiple dependencies using Seq and include both the existing and new libraries
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "0.7.29" % Test, // Existing test dependency
      "com.lihaoyi" %% "os-lib" % "0.9.3", // Adding os-lib for file and OS operations
      "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.0" % "provided",
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided"

)
  )
