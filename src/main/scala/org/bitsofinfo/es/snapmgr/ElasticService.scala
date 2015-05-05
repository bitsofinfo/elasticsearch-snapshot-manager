package org.bitsofinfo.es.snapmgr

import scala.collection.immutable._

trait ElasticService {

    def discoverNodes():List[Node]
    def getPrimaryNodesForIndex(indexName:String):List[Node]

    def getRepositories(repoNames:Seq[String]):List[Repository]
    def getRepository(repoName:String):Repository
    def createFSRepository(repoName:String, verify:Boolean, compress:Boolean, location:String):Boolean
    def deleteRepository(repoName:String):Boolean

    def createSnapshot(repoName:String, snapshotName:String, indexName:String):SnapshotResult
    def getSnapshots(repoName:String, snapshotNames:Seq[String]):List[Snapshot]
    def getSnapshot(repoName:String, snapshotName:String):Snapshot
}
