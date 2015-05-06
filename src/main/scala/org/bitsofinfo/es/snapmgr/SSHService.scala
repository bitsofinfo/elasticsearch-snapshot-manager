package org.bitsofinfo.es.snapmgr

import com.decodified.scalassh._

trait SSHService {

    def downloadFile(hostname:String, credentials:UnamePwCredential, remoteFilePath:String, targetLocalFilePath:String):Either[String,Unit]
    def uploadFile(hostname:String, credentials:UnamePwCredential, localFilePath:String, targetRemoteFilePath:String):Unit
    def remoteFileExists(hostname:String, credentials:UnamePwCredential, remoteFilePath:String):Boolean
    def execute(hostname:String, credentials:UnamePwCredential, command:String):Unit
}


case class UnamePwCredential(username:String, password:String)
