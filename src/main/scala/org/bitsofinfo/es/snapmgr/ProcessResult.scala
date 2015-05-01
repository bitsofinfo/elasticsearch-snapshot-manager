package org.bitsofinfo.es.snapmgr

class ProcessResult(val node:Node, val message:String, val success:Boolean, val error:Throwable) {

    override def toString():String = {
        "node:" + node + " success:" + success + " message:" + message + " error:"+error
    }

}
