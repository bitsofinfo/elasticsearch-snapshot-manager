package org.bitsofinfo.es.snapmgr

class SnapshotResult(val repoName:String,
                     val snapshotName:String,
                     val indexName:String,
                     val result:String,
                     val message:String,
                     val successfulShards:Int = 0,
                     val startTime:Long = 0,
                     val endTime:Long = 0) {

    override def toString():String = {
        ("repoName:" + repoName + " snapshotName:" + snapshotName + " indexName:"+indexName +
        " result:" + result + " message:" + message + " successfulShards:" +successfulShards +
        " startTime:"+startTime + " endTime:"+endTime)
    }

}
