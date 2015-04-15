package org.bitsofinfo.es.snapmgr

class Node(val nodeName:String, val address:String, val dataNode:Boolean) {

    override def toString():String = {
        "DataNode?:" + dataNode + " Name:" + nodeName + " address:"+address
    }

}
