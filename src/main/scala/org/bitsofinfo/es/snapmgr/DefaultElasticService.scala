package org.bitsofinfo.es.snapmgr

import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.immutable._

import scala.collection.mutable.{ ListBuffer }

import scala.concurrent.duration._

import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.admin._
import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.common.transport._
import org.elasticsearch.action.admin.indices.recovery._
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse
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

import org.bitsofinfo.es.snapmgr.util.getExceptionMessages

class DefaultElasticService(val esSeedHost:String, val esClusterName:String) extends ElasticService {

    val logger = LoggerFactory.getLogger(getClass)

    // map of ES client settings, all we care about is cluster.name
    val clientSettings = ImmutableSettings.settingsBuilder.put(mapAsJavaMap(Map("cluster.name"->esClusterName))).build

    // create our ES client instance
    val client = ElasticClient.remote(clientSettings,esSeedHost,9300)


    def discoverNodes():List[Node] = {

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

    /**
    * Attempt to get the list of nodes who hold primary shards for
    * the given indexName. Currently can't get this to reliably work due to:
    *
    * https://groups.google.com/d/msg/elasticsearch/vrzmZpADmhc/MiADvtlBMzoJ4
    **/
    def getPrimaryNodesForIndex(indexName:String):List[Node] = {

        val nodeMap = scala.collection.mutable.Map[String,Node]()
        discoverNodes().foreach(node => {
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


    def getRepositories(repoNames:Seq[String]):List[Repository] = {

        val listReposRequest = new GetRepositoriesRequest(repoNames.toArray)
        val listReposResponse = client.admin.cluster.getRepositories(listReposRequest).get()

        val repos = ListBuffer[Repository]()

        for(metaData <- listReposResponse.iterator) {
            repos += new Repository(metaData.name,metaData.settings.get("location"))
        }

        repos.toList
    }

    def getRepository(repoName:String):Repository = {
        val foundRepos = getRepositories(Seq(repoName))
        foundRepos.head
    }

    def createFSRepository(repoName:String, verify:Boolean, compress:Boolean, location:String):Boolean = {

        val putRepoRequest = new PutRepositoryRequest(repoName)
        putRepoRequest.`type`("fs").verify(verify)
        val settings = ImmutableSettings.settingsBuilder.put(mapAsJavaMap(Map("compress"->compress.toString, "location"->location))).build
        putRepoRequest.settings(settings)

        val putRepoResponse = client.admin.cluster.putRepository(putRepoRequest).get()
        putRepoResponse.isAcknowledged

    }

    def deleteRepository(repoName:String):Boolean = {

        val deleteRepoRequest = new DeleteRepositoryRequest(repoName)
        val deleteRepoResponse = client.admin.cluster.deleteRepository(deleteRepoRequest).get()
        deleteRepoResponse.isAcknowledged

    }

    def createSnapshot(repoName:String, snapshotName:String, indexName:String):SnapshotResult = {

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

    def getSnapshots(repoName:String, snapshotNames:Seq[String]):List[Snapshot] = {

        val response:GetSnapshotsResponse = client.execute {
            com.sksamuel.elastic4s.ElasticDsl.get snapshot snapshotNames from repoName
            }.await(Duration(30000,MILLISECONDS))

        val snapshots = ListBuffer[Snapshot]()

        for(info <- response.getSnapshots()) {
            snapshots += new Snapshot(repoName,info.name(),info.indices()(0),info.successfulShards(),info.startTime,info.endTime)
        }

        snapshots.toList
    }

    def getSnapshot(repoName:String, snapshotName:String):Snapshot = {
        val foundSnapshots = getSnapshots(repoName,Seq(snapshotName))
        foundSnapshots.head
    }

}
