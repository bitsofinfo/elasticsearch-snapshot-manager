package org.bitsofinfo.es.snapmgr

package object util {

    def getExceptionMessages(e:Throwable, msg:String):String = {
        if (e.getCause != null) getExceptionMessages(e.getCause, (msg +"\n"+e.getMessage)) else (msg+"\n"+e.getMessage)
    }
}
