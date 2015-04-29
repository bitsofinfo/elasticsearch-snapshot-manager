lazy val root = (project in file(".")).
  settings(
    name := "elasticsearch-snapshot-manager",
    version := "1.0",
    scalaVersion := "2.11.4"
  )


 //libraryDependencies += "fr.janalyse"   %% "janalyse-ssh" % "0.9.18" % "compile"


 libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "com.decodified" %% "scala-ssh" % "0.7.0",
  "org.bouncycastle" % "bcprov-jdk16" % "1.46",
  "com.jcraft" % "jzlib" % "1.1.3",
  "com.sksamuel.elastic4s" %% "elastic4s" % "1.5.5",
  "org.json4s" %% "json4s-native" % "3.2.11"
)

//SSH("xx.xx.xx.xx") { client =>
 // client.exec("ls -a").right.map { result =>
//    println("Result:\n" + result.stdOutAsString())
 // }
//}
