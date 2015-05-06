package org.bitsofinfo.es.snapmgr


import scala.collection.JavaConversions._
import scala.collection.immutable._

import scala.io.Source

import org.slf4j.LoggerFactory
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{Success, Failure}
import java.util.concurrent.Executors
import scala.collection.mutable.{ ListBuffer }


import scala.io.StdIn

import scala.concurrent.duration._

import org.json4s._
import org.json4s.native.JsonMethods._

import java.io._

import org.bitsofinfo.es.snapmgr.util.getExceptionMessages


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
class SnapshotManager(val esSeedHost:String, val esClusterName:String) {

    val logger = LoggerFactory.getLogger(getClass)

    val esService:ElasticService = new DefaultElasticService(esSeedHost,esClusterName)

    val sshService = new DefaultSSHService()

    def buildSnapshotMetadataManifest(snapshot:Snapshot):List[String] = {

        // build manifest relative from repositoryRoot
        val manifest = ListBuffer[String]()

        // the following 4 are only on master
        manifest += "index"
        manifest += "snapshot-" + snapshot.snapshotName
        manifest += "metadata-" + snapshot.snapshotName
        manifest += "indices/" + snapshot.indexName + "/snapshot-" + snapshot.snapshotName

        manifest.toList
    }

    def buildSnapshotSegmentsManifest(forNode:Node, forShard:Int, localSegmentsInfoFilePath:String, snapshot:Snapshot):List[String] = {

        val snapshotName = snapshot.snapshotName

        // build manifest relative from repositoryRoot
        val manifest = ListBuffer[String]()

        // if actual shard segment info file exists... build that...
        if (new java.io.File(localSegmentsInfoFilePath).exists) {

            logger.debug("Building snapshot segments manifest for shard: " + forShard + " on node: " + forNode.address + ":" + forNode.port)

            manifest += "indices/" + snapshot.indexName + "/" + forShard + "/snapshot-" + snapshotName

            // localSegmentsInfoFilePath: parse it from JSON, get the 'files' array, to collect each segment filename
            // then add each segment filename to the manifest under the same path
            val segmentFiles = getSegmentFiles(localSegmentsInfoFilePath)
            logger.info("Node: " + forNode.address+":"+forNode.port + " shard " + forShard + " contains " + segmentFiles.length + " snapshot segment files...")
            for (segmentFile <- segmentFiles) {
                manifest += "indices/" + snapshot.indexName + "/" + forShard + "/" + segmentFile.name
            }

        }

        manifest.toList
    }


    def createAndDownloadSnapshotTarball(hostname:String,
                                         credentials:UnamePwCredential,
                                         repositoryRoot:String,
                                         localManifestFilePath:String,
                                         remoteManifestFilePath:String,
                                         remoteTarballFileName:String,
                                         remoteTarballTargetDir:String,
                                         localTarballTargetDir:String):Unit = {

        // upload to the node
        sshService.uploadFile(hostname,credentials,localManifestFilePath,remoteManifestFilePath)

        // tar up the files
        val remoteSnapshotTarballPath = remoteTarballTargetDir + "/" + remoteTarballFileName
        sshService.execute(hostname, credentials, "cd " + repositoryRoot + "; tar -cvzf " + remoteSnapshotTarballPath + " -T " + remoteManifestFilePath)

        // download the [targetDir]/x.tar to local directory
        sshService.downloadFile(hostname,credentials,remoteSnapshotTarballPath,localTarballTargetDir)

        logger.info("Downloaded tarball from: " + hostname + " stored locally @ " + localTarballTargetDir + "/" + remoteTarballFileName)

    }

    def createManifestFile(manifestFilePath:String, manifestPaths:List[String]):Unit = {

        val writer = new PrintWriter(new File(manifestFilePath))

        for(file <- manifestPaths) {
            writer.write(file+"\n")
        }

        writer.close
    }


    def getSegmentFiles(pathToShardSnapshotInfoFile:String):List[SnapshotFile] = {

        logger.debug("getSegmentFiles() reading: " + pathToShardSnapshotInfoFile)

        implicit val formats = DefaultFormats

        val source = scala.io.Source.fromFile(pathToShardSnapshotInfoFile)
        val lines = try source.mkString finally source.close()
        val asJsonObj = parse(lines)
        val files = asJsonObj \\ "files"

        files.extract[List[SnapshotFile]]
    }



    def collectAllSnapshotTarballs(workDir:String, minutesToWait:Long):Unit = {

        // get repository
        val repos = esService.getRepositories(Seq("_all"))
        println("Available snapshot repositories:")
        repos.foreach(r => {
            println("    -> " + r.toString)
        })

        // collect their choice and validate what they typed is legit
        val repositoryName  = getUserInput(prompt="Snapshot repository name: ",
                                    validator=((input:String) => { !(repos.filter(r => r.name == input).isEmpty)}))
        val repository = esService.getRepository(repositoryName)


        // get snapshots
        val snapshots = esService.getSnapshots(repositoryName, Seq())
        println("Available snapshots within '"+repositoryName+"':")
        snapshots.foreach(s => {
            println("    -> " + s.toString)
        })

        // collect their choice and validate what they typed is legit
        val snapshotName = getUserInput(prompt="Snapshot name to be downloaded: ",
                                validator=((input:String) => { !(snapshots.filter(s => s.snapshotName == input).isEmpty)}))
        val snapshot = esService.getSnapshot(repositoryName,snapshotName)

        // collect SSH credentials
        val username = getUserInput(prompt="Enter SSH username: ", secure=false)
        val password = getUserInput(prompt="Enter SSH password: ", secure=true)

        val ready = getUserInput("Ready to proceed? (y/n): ");
        if (ready.trim != "y") {
            logger.info("Terminating process, received something other than 'y'")
            System.exit(0)
        }

        // get all nodes, build manifest files
        // and download tarballs concurrently
        logger.info("Discovering elasticsearch cluster nodes....")
        val allNodes = esService.discoverNodes()
        allNodes.foreach(n => logger.info("Discovered: " + n))

        val workers = new ListBuffer[Future[ProcessResult]]

        allNodes.foreach(node => {
            workers += processNode(node,repository,snapshot,workDir,username,password);
        })

        workers.foreach(w => {
            // there is some bug where if this block is called within
            // the foreach above.. it causes 2 threads to be spawned per
            // call to processNode for each node...
            w.onComplete {
                case Success(processResult) => logger.info(processResult.toString)
                case Failure(e) => logger.error("NODE PROCESS EXCEPTION: " + e.getMessage,e)
            }
        })

        workers.foreach(w => Await.result(w,minutesToWait minutes))

        logger.info("COMPLETE! All downloaded tarballs located @ " + workDir)

    }

    def processShard(node:Node, repository:Repository, snapshot:Snapshot,
                     shardNum:Int, workDir:String, localShardSegmentsInfoFile:String, credential:UnamePwCredential):Future[ProcessResult] = {

        future {

            logger.info("Processing: " + node.address + " shard["+shardNum+"]")

            try {
                logger.info("Node: " + node.address +":" + node.port + " shard["+shardNum+"] contains snapshot segments.. proceeding to download...")

                // create manifest file for shard
                val localNodeManifestFile = workDir+"/"+node.address+"-"+node.port+"-shard"+shardNum+"-snapshot-segments.manifest"
                val manifest = buildSnapshotSegmentsManifest(node,shardNum,localShardSegmentsInfoFile,snapshot)
                createManifestFile(localNodeManifestFile,manifest)

                // create and download
                createAndDownloadSnapshotTarball(node.address,
                                                 credential,
                                                 repository.location,
                                                 localNodeManifestFile,
                                                 "/tmp/snapshotManager-tarball-manifest-s" + shardNum + "-" +java.util.UUID.randomUUID.toString,
                                                 node.address + "_"+ node.port + "-" + repository.name + "-"
                                                    + snapshot.snapshotName + "-" + snapshot.indexName+"-s" + shardNum+ "-shard-files.tar.gz",
                                                 "/tmp",
                                                 workDir)

                new ProcessResult(node,"Node["+node.address+":"+node.port+"].Shard["+shardNum+"] segment data downloaded ok",true,null)


            } catch {
                case e:Exception => {
                    new ProcessResult(node,
                                      "Unexpected error: " + getExceptionMessages(e,""),
                                      false,e)
                }
            }

        }
    }

    def processNode(node:Node, repository:Repository, snapshot:Snapshot,
                    workDir:String, username:String, password:String):Future[ProcessResult] = {

        future {

            try {

                logger.info("Processing node: " + node.address)

                val credential = new UnamePwCredential(username,password)

                // does this node contain the snapshot meta-data files?
                val metaDataFilesExist = sshService.remoteFileExists(node.address,credential,repository.location + "/metadata-"+snapshot.snapshotName)
                if (metaDataFilesExist) {
                    val localNodeManifestFile = workDir+"/"+node.address+"-"+node.port+"-snapshot-metadata.manifest"
                    val metadataManifest = buildSnapshotMetadataManifest(snapshot)
                    createManifestFile(localNodeManifestFile,metadataManifest)

                    // create and download
                    createAndDownloadSnapshotTarball(node.address,
                                                     credential,
                                                     repository.location,
                                                     localNodeManifestFile,
                                                     "/tmp/snapshotManager-tarball-manifest-metadata-" +java.util.UUID.randomUUID.toString,
                                                     node.address + "_"+ node.port + "-" + repository.name + "-"
                                                        + snapshot.snapshotName + "-" +snapshot.indexName+"-metadata-files.tar.gz",
                                                     "/tmp",
                                                     workDir)
                }


                // connect and download
                val shardWorkers = new ListBuffer[Future[ProcessResult]]

                // next we attempt to process shard segment data per node
                for (shardNum <- (0 to (snapshot.successfulShards-1))) {

                    val localShardSegmentsInfoFile = workDir+"/"+node.address+"-"+node.port+"-shard"+shardNum+"-snapshot-"+snapshot.snapshotName

                    // pull segments meta-data file
                    val segmentsFile = repository.location + "/indices/" + snapshot.indexName + "/" + shardNum + "/snapshot-" + snapshot.snapshotName
                    sshService.downloadFile(node.address,credential,segmentsFile,localShardSegmentsInfoFile)

                    // only if the node we connected to actually has some data (metadata || shard segment data)...
                    if (new java.io.File(localShardSegmentsInfoFile).exists) {
                        shardWorkers += processShard(node, repository, snapshot, shardNum, workDir, localShardSegmentsInfoFile, credential)
                    }
                }


                shardWorkers.foreach(w => {
                    // there is some bug where if this block is called within
                    // the foreach above.. it causes 2 threads to be spawned per
                    // call to processNode for each node...
                    w.onComplete {
                        case Success(processResult) => logger.info(processResult.toString)
                        case Failure(e) => logger.error("SHARD PROCESS EXCEPTION: " + e.getMessage,e)
                    }
                })

                shardWorkers.foreach(w => Await.result(w,60 minutes))

                logger.info("NODE ("+node.address+") SHARDS COMPLETE!")

                new ProcessResult(node,"Node["+node.address+":"+node.port+"] completed ok",true,null)

            } catch {
                case e:Exception => {
                    new ProcessResult(node,
                                      "Unexpected error: " + getExceptionMessages(e,""),
                                      false,e)
                }
            }


            }

    }

    def getUserInput(prompt:String, validator:(String) => Boolean = (input:String) => true, secure:Boolean=false):String = {

        var input:String = null;

        while(input == null || input.trim.isEmpty() || !validator(input)) {
            print(prompt);
            if (secure) {
                input = new String(System.console.readPassword())
            } else {
                input = StdIn.readLine()
            }
            println()
        }

        return input;
    }

}
