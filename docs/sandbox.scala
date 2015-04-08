import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest
import scala.collection.JavaConversions._
import scala.collection.mutable._
import org.elasticsearch.cluster.node._
import org.elasticsearch.common.transport.TransportAddress

val client = ElasticClient.remote("localhost",9300)
val clusterStateRequest = new ClusterStateRequest().nodes(true)
val clusterStateResponse = client.admin.cluster.state(clusterStateRequest).get()

var discoveryNodes = clusterStateResponse.getState.getNodes()
var nodeId2DiscoveryNodeMap = discoveryNodes.nodes

val nodes = ArrayBuffer[DiscoveryNode]()
val ips = ArrayBuffer[TransportAddress]()

for(kv <- nodeId2DiscoveryNodeMap.iterator) {
    nodes += kv.value
    ips += kv.value.getAddress
}

nodes.foreach((dn) => {println(dn)})
ips.foreach((dn) => {println(dn)})
