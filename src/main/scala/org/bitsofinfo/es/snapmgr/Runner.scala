package org.bitsofinfo.es.snapmgr

object Runner {
    def main(args: Array[String]) = {

        if (args.length < 2) {
            println("Usage java -jar [jarname] [elasticSearch hostname] [local workDir]")
            System.exit(1)
        }

        def sm = new SnapshotManager(args(0))
        sm.collectAllSnapshotTarballs(sm.client,args(1))
    }

}
