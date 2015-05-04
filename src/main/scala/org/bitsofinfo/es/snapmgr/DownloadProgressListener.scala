package org.bitsofinfo.es.snapmgr

import net.schmizz.sshj.xfer.TransferListener
import net.schmizz.sshj.common.StreamCopier
import org.slf4j.LoggerFactory

class DownloadProgressListener(val logPrefix:String,
                               val reportEveryNSeconds:Int,
                               val myPath:String="",
                               val mySize:Long=0) extends TransferListener
                                                  with StreamCopier.Listener {

    val logger = LoggerFactory.getLogger(getClass)

    override def directory(name:String):TransferListener = {
        val newTarget = myPath concat name;
        logger.debug(logPrefix + "started transferring directory " + newTarget);
        new DownloadProgressListener(logPrefix,reportEveryNSeconds,newTarget)
    }

    override def file(name:String, size:Long):StreamCopier.Listener = {

        val newTarget = myPath concat name;

        logger.debug(logPrefix + " started transferring file: " + newTarget + ", size="+size);
        new DownloadProgressListener(logPrefix,reportEveryNSeconds,newTarget,size)
    }

    override def reportProgress(transferred:Long):Unit = {
        logger.debug(logPrefix + " transferred " + ((transferred * 100) / mySize) + "% of " + myPath);
    }

}
