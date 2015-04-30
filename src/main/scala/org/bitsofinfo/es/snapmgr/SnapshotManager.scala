package org.bitsofinfo.es.snapmgr

import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.admin._
import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.common.transport._
import org.elasticsearch.action.admin.indices.recovery._
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse
import scala.collection.JavaConversions._
import scala.collection.immutable._

import scala.io.Source

import scala.collection.mutable.{ ListBuffer }
import org.elasticsearch.cluster.node._
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.cluster.metadata.RepositoryMetaData
import org.elasticsearch.action.admin.cluster.repositories.get._
import org.elasticsearch.action.admin.cluster.repositories.put._
import org.elasticsearch.action.admin.cluster.repositories.delete._
import org.elasticsearch.action.admin.cluster.repositories._
import org.elasticsearch.common.settings._
import org.elasticsearch.common.settings.ImmutableSettings.Builder

import org.elasticsearch.transport.RemoteTransportException
import org.elasticsearch.snapshots.InvalidSnapshotNameException

import com.decodified.scalassh._

import scala.io.StdIn

import scala.concurrent.duration._

import org.json4s._
import org.json4s.native.JsonMethods._

import java.io._



// [repositoryRoot]/index
//      {"snapshots":["snap99","snap102","vectorprime","snap1","snap55","snap56","snap57","ls1","snap100","snap101"]}

// [repositoryRoot]/snapshot-[snapshotName]
//     {"snapshot":{"name":"ls1","version_id":1050299,"indices":["logstash-2015.02.24"],"state":"SUCCESS","start_time":1430316763665,"end_time":1430316862727,"total_shards":5,"successful_shards":5,"failures":[]}}

// [repositoryRoot]/metadata-[snapshotName]
//     {"meta-data":{"version":74,"uuid":"Q7Oabmf-Tz-wVIix-hTEkg","templates":{}}}

// [repositoryRoot]/indices/[indexName]/snapshot-[snapshotName]
//     JSON file contains all index settings/config etc

// [repositoryRoot]/indices/[indexName]/[shard#]/snapshot-[snapshotName]

/*     Contains list of all segment files within the shard directory
       that are part of the snapshot
    {
      "name" : "ls1",
      "index_version" : 83,
      "start_time" : 1430316763701,
      "time" : 97221,
      "number_of_files" : 289,
      "total_size" : 2052566131,
      "files" : [ {
        "name" : "__0",
        "physical_name" : "_mlm_Lucene410_0.dvd",
        "length" : 404,
        "checksum" : "1fyhsxd",
        "written_by" : "4.10.3"
      }, {
        "name" : "__1",
        "physical_name" : "_mqc_Lucene410_0.dvm",
        "length" : 91,
        "checksum" : "a9bz95",
        "written_by" : "4.10.3"
      }, {
        "name" : "__2",
        "physical_name" : "_m62.si",
        "length" : 455,
        "checksum" : "14b1wg0",
        "written_by" : "4.10.3",
        "meta_hash" : "P9dsFxNMdWNlbmU0NlNlZ21lbnRJbmZvAAAAAQY0LjEwLjMAAFHn/wAAAAoJdGltZXN0YW1wDTE0MjQ4MjExNTUyMjMLbWVyZ2VGYWN0b3ICMTAKb3MudmVyc2lvbhoyLjYuMzItNDMxLjE3LjEuZWw2Lng4Nl82NAJvcwVMaW51eA5sdWNlbmUudmVyc2lvbgY0LjEwLjMGc291cmNlBW1lcmdlB29zLmFyY2gFYW1kNjQTbWVyZ2VNYXhOdW1TZWdtZW50cwItMQxqYXZhLnZlcnNpb24IMS43LjBfNTULamF2YS52ZW5kb3IST3JhY2xlIENvcnBvcmF0aW9uAAAADRBfbTYyX2VzMDkwXzAudGlwFF9tNjJfTHVjZW5lNDEwXzAuZHZkB19tNjIuc2kQX202Ml9lczA5MF8wLmRvYxRfbTYyX0x1Y2VuZTQxMF8wLmR2bQhfbTYyLm52bRBfbTYyX2VzMDkwXzAuYmxtCF9tNjIubnZkCF9tNjIuZmR0EF9tNjJfZXMwOTBfMC5wb3MIX202Mi5mZHgQX202Ml9lczA5MF8wLnRpbQhfbTYyLmZubcAok+gAAAAAAAAAAJFE5YA="
    }, ........


    import org.bitsofinfo.es.snapmgr._
    val sm = new SnapshotManager
    val snapshot = sm.getSnapshot(sm.client,"dog","ls1")
    val hostconfig = sm.promptForHostConfigProvider
    sm.createManifestFile("/tmp/manifest1",sm.buildSnapshotManifest(sm.client,"/tmp/dog",snapshot))
    sm.createSnapshotTarball("localhost",hostconfig,"/tmp/dog","/tmp/manifest1","/tmp","/tmp/zz2")

*/
class SnapshotManager(val esSeedHost:String) {

    val client = ElasticClient.remote(esSeedHost,9300)

    def discoverNodes(client:ElasticClient):List[Node] = {

        def toIP(addr: TransportAddress):String = addr match {
            case addr:InetSocketTransportAddress => addr.address.getHostName
            case _ => addr.toString
        }

        def toPort(addr: TransportAddress):Int = addr match {
            case addr:InetSocketTransportAddress => addr.address.getPort
            case _ => -1
        }

        val clusterStateRequest = new ClusterStateRequest().nodes(true)
        val clusterStateResponse = client.admin.cluster.state(clusterStateRequest).get()

        var discoveryNodes = clusterStateResponse.getState.getNodes()
        var nodeId2DiscoveryNodeMap = discoveryNodes.nodes

        val nodes = ListBuffer[Node]()

        for(kv <- nodeId2DiscoveryNodeMap.iterator) {

            val node:DiscoveryNode = kv.value

            nodes += new Node(node.id, node.name, toIP(node.address), node.isDataNode, toPort(node.address))
        }

        nodes.toList
    }

    // https://groups.google.com/d/msg/elasticsearch/vrzmZpADmhc/MiADvtlBMzoJ4
    def getPrimaryNodesForIndex(client:ElasticClient, indexName:String):List[Node] = {

        val nodeMap = scala.collection.mutable.Map[String,Node]()
        discoverNodes(client).foreach(node => {
            nodeMap += (node.nodeId -> node)
        })

        val primaryNodes = ListBuffer[Node]()

        val response:RecoveryResponse = client.execute { recover index indexName }.await

        response.shardResponses.values.foreach(srrList => {
                srrList.foreach(shardRecoveryResponse => {

                    val recoveryState = shardRecoveryResponse.recoveryState;
                    println(recoveryState.getType)
                    if (recoveryState.getPrimary) {
                        primaryNodes += nodeMap(recoveryState.getSourceNode.getId)
                    }
                })
        })

        primaryNodes.toList
    }


    def getRepositories(client:ElasticClient, repoNames:Seq[String] = Seq("_all")):List[Repository] = {

        val listReposRequest = new GetRepositoriesRequest(repoNames.toArray)
        val listReposResponse = client.admin.cluster.getRepositories(listReposRequest).get()

        val repos = ListBuffer[Repository]()

        for(metaData <- listReposResponse.iterator) {
            repos += new Repository(metaData.name,metaData.settings.get("location"))
        }

        repos.toList
    }

    def getRepository(client:ElasticClient, repoName:String):Repository = {
        val foundRepos = getRepositories(client, Seq(repoName))
        foundRepos.head
    }

    def createFSRepository(client:ElasticClient, repoName:String, verify:Boolean, compress:Boolean, location:String):Boolean = {

        val putRepoRequest = new PutRepositoryRequest(repoName)
        putRepoRequest.`type`("fs").verify(verify)
        val settings = ImmutableSettings.settingsBuilder.put(mapAsJavaMap(Map("compress"->compress.toString, "location"->location))).build
        putRepoRequest.settings(settings)

        val putRepoResponse = client.admin.cluster.putRepository(putRepoRequest).get()
        putRepoResponse.isAcknowledged

    }

    def deleteRepository(client:ElasticClient, repoName:String):Boolean = {

        val deleteRepoRequest = new DeleteRepositoryRequest(repoName)
        val deleteRepoResponse = client.admin.cluster.deleteRepository(deleteRepoRequest).get()
        deleteRepoResponse.isAcknowledged

    }

    def createSnapshot(client:ElasticClient, repoName:String, snapshotName:String, indexName:String):SnapshotResult = {

        try {
            val response:CreateSnapshotResponse = client.execute {
                snapshot create snapshotName in repoName waitForCompletion true index indexName
            }.await(Duration(30000,MILLISECONDS))

            val info = response.getSnapshotInfo

            new SnapshotResult(repoName,snapshotName,indexName,info.state.toString,info.state.toString,info.successfulShards,info.startTime,info.endTime)

        } catch {
            case e:Exception => {
                new SnapshotResult(repoName,snapshotName,indexName,"ERROR",getExceptionMessages(e,""))
            }
        }
    }


    def getSnapshots(client:ElasticClient, repoName:String, snapshotNames:Seq[String] = Seq()):List[Snapshot] = {

        val response:GetSnapshotsResponse = client.execute {
            com.sksamuel.elastic4s.ElasticDsl.get snapshot snapshotNames from repoName
            }.await(Duration(30000,MILLISECONDS))

        val snapshots = ListBuffer[Snapshot]()

        for(info <- response.getSnapshots()) {
            snapshots += new Snapshot(repoName,info.name(),info.indices()(0),info.successfulShards(),info.startTime,info.endTime)
        }

        snapshots.toList
    }

    def getSnapshot(client:ElasticClient, repoName:String, snapshotName:String):Snapshot = {
        val foundSnapshots = getSnapshots(client,repoName,Seq(snapshotName))
        foundSnapshots.head
    }

    def promptForHostConfigProvider():HostConfigProvider = {
        print("Enter target SSH host : ")
        val host = StdIn.readLine()
        println()
        print("Enter SSH username: ")
        val username = StdIn.readLine()
        println()
        print("Enter SSH password: ")
        val password = StdIn.readLine()
        println()
        HostConfigProvider.hostConfig2HostConfigProvider(HostConfig(login=PasswordLogin(username,password), hostName=host, hostKeyVerifier=HostKeyVerifiers.DontVerify))
    }


    def getExceptionMessages(e:Throwable, msg:String):String = {
        if (e.getCause != null) getExceptionMessages(e.getCause, (msg +"\n"+e.getMessage)) else (msg+"\n"+e.getMessage)
    }


    def buildSnapshotManifest(client:ElasticClient, localSegmentsInfoFilePath:String, snapshot:Snapshot):List[String] = {

        val snapshotName = snapshot.snapshotName


        // build manifest relative from repositoryRoot
        val manifest = ListBuffer[String]()

        // the following 4 are only on master
        manifest += "index"
        manifest += "snapshot-" + snapshotName
        manifest += "metadata-" + snapshotName
        manifest += "indices/" + snapshot.indexName + "/snapshot-" + snapshotName

        // if actual shard segment info file exists... build that...
        if (new java.io.File(localSegmentsInfoFilePath).exists) {
            for (shardNum <- (0 to (snapshot.successfulShards-1))) {

                manifest += "indices/" + snapshot.indexName + "/" + shardNum + "/snapshot-" + snapshotName

                // localSegmentsInfoFilePath: parse it from JSON, get the 'files' array, to collect each segment filename
                // then add each segment filename to the manifest under the same path
                for (segmentFile <- getSegmentFiles(localSegmentsInfoFilePath)) {
                    manifest += "indices/" + snapshot.indexName + "/" + shardNum + "/" + segmentFile.name
                }
            }

        }


        manifest.toList

    }

    def downloadFile(hostname:String, hostConfigProvider:HostConfigProvider, remoteFilePath:String, targetLocalFilePath:String):Either[String,Unit] = {
        SSH(hostname,hostConfigProvider) { client =>
            client.download(remoteFilePath,targetLocalFilePath)
        }
    }

    def remoteFileExists(hostname:String, hostConfigProvider:HostConfigProvider, remoteFilePath:String):Boolean = {

        SSH(hostname,hostConfigProvider) { client =>
            client.exec("[ -f "+remoteFilePath+" ] && echo \"_FOUND_\" || echo \"Not found\"").right.map { result =>
                result.stdOutAsString().indexOf("_FOUND_") != -1
            }
        }.right.get

    }

    def createdAndDownloadSnapshotTarball(hostname:String,
                                          nodePort:Int,
                                          repoName:String,
                                          snapshotName:String,
                                          indexName:String,
                                          hostConfigProvider:HostConfigProvider,
                                          repositoryRoot:String,
                                          localManifestFilePath:String,
                                          remoteTarballTargetDir:String,
                                          localTarballTargetDir:String):Either[String,Unit] = {

        SSH(hostname,hostConfigProvider) { client =>

            val remoteManifestFilePath = "/tmp/snapshotManager-tarball-manifest-" + java.util.UUID.randomUUID.toString

            // upload to the node
            client.upload(localManifestFilePath,remoteManifestFilePath)

            // tar up the files
            val remoteSnapshotTarballPath = remoteTarballTargetDir + "/" + hostname + "_"+ nodePort + "-" + repoName + "-" + snapshotName + "-" +indexName+"-snapshotfiles.tar.gz"
            client.exec("cd " + repositoryRoot + "; tar -cvzf " + remoteSnapshotTarballPath + " -T " + remoteManifestFilePath)

            // download the [targetDir]/x.tar to local directory
            client.download(remoteSnapshotTarballPath,localTarballTargetDir)
        }
    }

    def createManifestFile(manifestFilePath:String, manifestPaths:List[String]):Unit = {

        val writer = new PrintWriter(new File(manifestFilePath))

        for(file <- manifestPaths) {
            writer.write(file.name+"\n")
        }

        writer.close
    }


    def getSegmentFiles(pathToShardSnapshotInfoFile:String):List[SnapshotFile] = {

        println("getSegmentFiles() reading: " + pathToShardSnapshotInfoFile)

        implicit val formats = DefaultFormats

        val source = scala.io.Source.fromFile(pathToShardSnapshotInfoFile)
        val lines = try source.mkString finally source.close()
        val asJsonObj = parse(lines)
        val files = asJsonObj \\ "files"

        files.extract[List[SnapshotFile]]
    }



    def collectAllSnapshotTarballs(client:ElasticClient, workDir:String):Unit = {

        // get repository
        val repos = getRepositories(client)
        println("Available snapshot repositories:")
        repos.foreach(r => {
            println("\t" + r.toString)
        })
        print("Enter repository name: ")
        val repositoryName = StdIn.readLine()
        println()
        val repository = getRepository(client,repositoryName)

        // get snapshots
        val snapshots = getSnapshots(client,repositoryName)
        println("Available snapshots within '"+repositoryName+"':")
        snapshots.foreach(s => {
            println("\t" + s.toString)
        })
        print("Enter snapshot to be downloaded: ")
        val snapshotName = StdIn.readLine()
        println()
        val snapshot = getSnapshot(client,repositoryName,snapshotName)

        // collect SSH credentials
        print("Enter SSH username: ")
        val username = StdIn.readLine()
        println()
        print("Enter SSH password: ")
        val password = StdIn.readLine()
        println()

        // get all nodes, build manifest files
        val allNodes = discoverNodes(client)
        allNodes.foreach(node => {

            println("Working on node: " + node)

            // build ssh config for this host
            val sshHostConfig = HostConfigProvider.hostConfig2HostConfigProvider(HostConfig(login=PasswordLogin(username,password), hostName=node.address, hostKeyVerifier=HostKeyVerifiers.DontVerify))

            // connect and download
            for (shardNum <- (0 to (snapshot.successfulShards-1))) {

                val localShardSegmentsInfoFile = workDir+"/"+node.address+"-"+node.port+"-shard"+shardNum+"-snapshot-"+snapshotName

                val metaDataFilesExist = remoteFileExists(node.address,sshHostConfig,repository.location + "/metadata-"+snapshot.snapshotName)

                val segmentsFile = repository.location + "/indices/" + snapshot.indexName + "/" + shardNum + "/snapshot-" + snapshotName
                downloadFile(node.address,
                             sshHostConfig,
                             segmentsFile,
                             localShardSegmentsInfoFile)

                // only if the node we connected to actually has some data (metadata || shard segment data)...
                if (metaDataFilesExist || new java.io.File(localShardSegmentsInfoFile).exists) {

                    // create manifest file for shard
                    val localNodeManifestFile = workDir+"/"+node.address+"-"+node.port+"-shard"+shardNum+"-snapshot.manifest"
                    createManifestFile(localNodeManifestFile,buildSnapshotManifest(client,localShardSegmentsInfoFile,snapshot))

                    // create and download
                    createdAndDownloadSnapshotTarball(node.address,
                                                     node.port,
                                                     repository.name,
                                                     snapshot.snapshotName,
                                                     snapshot.indexName,
                                                     sshHostConfig,
                                                     repository.location,
                                                     localNodeManifestFile,
                                                     "/tmp",
                                                     workDir)
                }


            }

        })

    }

}
