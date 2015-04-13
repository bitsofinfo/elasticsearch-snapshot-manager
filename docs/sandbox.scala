import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import scala.collection.JavaConversions._
import scala.collection.mutable._
import scala.collection.immutable._
import org.elasticsearch.cluster.node._
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.cluster.metadata.RepositoryMetaData
import org.elasticsearch.action.admin.cluster.repositories.get._
import org.elasticsearch.action.admin.cluster.repositories.put._
import org.elasticsearch.action.admin.cluster.repositories.delete._
import org.elasticsearch.action.admin.cluster.repositories._
import org.elasticsearch.common.settings._
import org.elasticsearch.common.settings.ImmutableSettings.Builder

import com.decodified.scalassh._

val client = ElasticClient.remote("localhost",9300)

def discoverNodes(client:ElasticClient):List[DiscoveryNode] = {

    val clusterStateRequest = new ClusterStateRequest().nodes(true)
    val clusterStateResponse = client.admin.cluster.state(clusterStateRequest).get()

    var discoveryNodes = clusterStateResponse.getState.getNodes()
    var nodeId2DiscoveryNodeMap = discoveryNodes.nodes

    val nodes = ListBuffer[DiscoveryNode]()

    for(kv <- nodeId2DiscoveryNodeMap.iterator) {
        nodes += kv.value
    }

    nodes.toList
}


def getRepositories(client:ElasticClient):List[RepositoryMetaData] = {

    val listReposRequest = new GetRepositoriesRequest(Array("_all"))
    val listReposResponse = client.admin.cluster.getRepositories(listReposRequest).get()

    val repos = ListBuffer[RepositoryMetaData]()

    for(metaData <- listReposResponse.iterator) {
        repos += metaData
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
