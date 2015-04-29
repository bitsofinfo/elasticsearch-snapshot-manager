package org.bitsofinfo.es.snapmgr

class Node(val nodeId:String, val nodeName:String, val address:String, val dataNode:Boolean) {

    override def toString():String = {
        "DataNode?:" + dataNode + " ID:" + nodeId + " Name:" + nodeName + " address:"+address
    }

}
