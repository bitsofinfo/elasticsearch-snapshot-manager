package org.bitsofinfo.es.snapmgr

class Node(val nodeId:String, val nodeName:String, val address:String, val dataNode:Boolean, val port:Int) {

    override def toString():String = {
        "Name:" + nodeName + " address:"+address + ":" + port + " id:" + nodeId +" DataNode?:" + dataNode 
    }

}
