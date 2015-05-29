package org.bitsofinfo.es.snapmgr

import org.slf4j.LoggerFactory
import com.decodified.scalassh._

class DefaultSSHService extends SSHService {

    val logger = LoggerFactory.getLogger(getClass)

    val HISTCONTROL = "export HISTCONTROL=ignorespace"

    def getSudoPrefix(sudo:Boolean, credentials:UnamePwCredential):String = {
        // sudo prefix (NOTE leading space! and HISTCONTROL exports....)
        if (sudo) " echo '"+credentials.password+"' | sudo -S " else ""
    }

    def getHostConfigProvider(hostname:String, credentials:UnamePwCredential) = {
        HostConfigProvider.hostConfig2HostConfigProvider(
            HostConfig(login=PasswordLogin(credentials.username,credentials.password),
            hostName=hostname, hostKeyVerifier=HostKeyVerifiers.DontVerify))
    }


    def downloadFile(hostname:String, credentials:UnamePwCredential, remoteFilePath:String, targetLocalFilePath:String):Either[String,Unit] = {

        SSH(hostname,getHostConfigProvider(hostname,credentials)) { client =>

            // node the 3rd param (listener: TransferListener) is
            // declared implicit, so we could also just define a global
            // variable of type TransferListener and it would pick it up
            client.download(remoteFilePath,targetLocalFilePath)(new DownloadProgressListener("",10))
        }
    }

    def remoteFileExists(hostname:String, credentials:UnamePwCredential, sudo:Boolean, remoteFilePath:String):Boolean = {

        val result = SSH(hostname,getHostConfigProvider(hostname,credentials)) { client =>

            val cmd = getSudoPrefix(sudo,credentials) +" [ -f "+remoteFilePath+" ] && echo \"_FOUND_\" || echo \"Not found\""

            if (sudo) {
                client.execPTY(HISTCONTROL)
                client.execPTY(cmd).right.map { result =>
                    result.stdOutAsString().indexOf("_FOUND_") != -1
                }
            } else {
                client.exec(cmd).right.map { result =>
                    result.stdOutAsString().indexOf("_FOUND_") != -1
                }
            }

        }

        if (result.isRight) {
            result.right.get
        } else {
            logger.error("remoteFileExists() error: " + result.left.get)
            throw new Exception("remoteFileExists() error: " + result.left.get)
        }

    }

    def uploadFile(hostname:String, credentials:UnamePwCredential, localFilePath:String, targetRemoteFilePath:String):Unit = {

        SSH(hostname,getHostConfigProvider(hostname,credentials)) { client =>

            client.upload(localFilePath,targetRemoteFilePath)
        }
    }


    def execute(hostname:String, credentials:UnamePwCredential, sudo:Boolean, command:String):Unit = {

        SSH(hostname,getHostConfigProvider(hostname,credentials)) { client =>

            val cmd = getSudoPrefix(sudo,credentials) + command

            if (sudo) {
                client.execPTY(HISTCONTROL)
                client.execPTY(cmd)
            } else {
                client.exec(cmd)
            }

        }
    }

    def execute(hostname:String, credentials:UnamePwCredential, sudo:Boolean, commands:Array[String]):Unit = {

        val sudoPrefix = getSudoPrefix(sudo,credentials)

        SSH(hostname,getHostConfigProvider(hostname,credentials)) { client =>

            commands.foreach(command => {

                val cmd = sudoPrefix + command

                if (sudo) {
                    client.execPTY(HISTCONTROL)
                    client.execPTY(cmd).right.map { result =>
                        //println(result.stdOutAsString())
                        //println(result.stdErrAsString())
                    }
                } else {
                    client.exec(cmd).right.map { result =>
                        //println(result.stdOutAsString())
                        //println(result.stdErrAsString())
                    }
                }

            })

        }
    }
}
