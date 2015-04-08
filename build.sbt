lazy val root = (project in file(".")).
  settings(
    name := "elasticsearch-snapshot-manager",
    version := "1.0",
    scalaVersion := "2.11.4"
  )


 //libraryDependencies += "fr.janalyse"   %% "janalyse-ssh" % "0.9.18" % "compile"

 libraryDependencies += "com.decodified" %% "scala-ssh" % "0.7.0"
 libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2"

 libraryDependencies ++= Seq(
  "org.bouncycastle" % "bcprov-jdk16" % "1.46",
  "com.jcraft" % "jzlib" % "1.1.3"
)

libraryDependencies += "com.sksamuel.elastic4s" %% "elastic4s" % "1.4.13"

//SSH("xx.xx.xx.xx") { client =>
 // client.exec("ls -a").right.map { result =>
//    println("Result:\n" + result.stdOutAsString())
 // }
//}
