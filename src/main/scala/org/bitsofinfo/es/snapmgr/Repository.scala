package org.bitsofinfo.es.snapmgr

class Repository(val name:String, val location:String) {

    override def toString():String = {
        "Name:" + name + " location:" + location
    }

}
