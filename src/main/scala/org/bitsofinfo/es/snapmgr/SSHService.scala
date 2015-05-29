package org.bitsofinfo.es.snapmgr

import com.decodified.scalassh._

trait SSHService {

    def downloadFile(hostname:String, credentials:UnamePwCredential, remoteFilePath:String, targetLocalFilePath:String):Either[String,Unit]
    def uploadFile(hostname:String, credentials:UnamePwCredential, localFilePath:String, targetRemoteFilePath:String):Unit
    def remoteFileExists(hostname:String, credentials:UnamePwCredential, sudoRemoteOps:Boolean, remoteFilePath:String):Boolean
    def execute(hostname:String, credentials:UnamePwCredential, sudo:Boolean, command:String):Unit
    def execute(hostname:String, credentials:UnamePwCredential, sudo:Boolean, commands:Array[String]):Unit
}


case class UnamePwCredential(username:String, password:String)
