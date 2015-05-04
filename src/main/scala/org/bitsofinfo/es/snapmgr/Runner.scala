package org.bitsofinfo.es.snapmgr

object Runner {
    def main(args: Array[String]) = {

        if (args.length < 2) {
            println("Usage java -jar [jarname] [elasticSearch hostname] [clusterName] [local workDir] [minutesToWaitPerDownload]")
            System.exit(1)
        }

        def sm = new SnapshotManager(args(0), args(1))
        sm.collectAllSnapshotTarballs(sm.client,args(2),args(3).toLong)
    }

}
