package org.bitsofinfo.es.snapmgr

import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.admin._
import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.common.transport._
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse
import scala.collection.JavaConversions._
import scala.collection.immutable._

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


class SnapshotManager {

    val client = ElasticClient.remote("localhost",9300)

    def discoverNodes(client:ElasticClient):List[Node] = {

        def toIP(addr: TransportAddress): String = addr match {
            case addr:InetSocketTransportAddress => addr.address.getHostName
            case _ => addr.toString
        }

        val clusterStateRequest = new ClusterStateRequest().nodes(true)
        val clusterStateResponse = client.admin.cluster.state(clusterStateRequest).get()

        var discoveryNodes = clusterStateResponse.getState.getNodes()
        var nodeId2DiscoveryNodeMap = discoveryNodes.nodes

        val nodes = ListBuffer[Node]()

        for(kv <- nodeId2DiscoveryNodeMap.iterator) {

            val node:DiscoveryNode = kv.value

            nodes += new Node(node.name, toIP(node.address), node.isDataNode)
        }

        nodes.toList
    }

    def getRepositories(client:ElasticClient):List[Repository] = {

        val listReposRequest = new GetRepositoriesRequest(Array("_all"))
        val listReposResponse = client.admin.cluster.getRepositories(listReposRequest).get()

        val repos = ListBuffer[Repository]()

        for(metaData <- listReposResponse.iterator) {
            repos += new Repository(metaData.name,metaData.settings.get("location"))
        }

        repos.toList

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
                println(e)
                new SnapshotResult(repoName,snapshotName,indexName,"ERROR",getExceptionMessages(e,""))
            }
        }
    }


    def getSnapshots(client:ElasticClient, repoName:String):List[Snapshot] = {

        val response:GetSnapshotsResponse = client.execute {
            com.sksamuel.elastic4s.ElasticDsl.get snapshot Seq() from repoName
            }.await(Duration(30000,MILLISECONDS))

        val snapshots = ListBuffer[Snapshot]()

        for(info <- response.getSnapshots()) {
            snapshots += new Snapshot(repoName,info.name(),info.indices()(0),info.successfulShards(),info.startTime,info.endTime)
        }

        snapshots.toList
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

}
