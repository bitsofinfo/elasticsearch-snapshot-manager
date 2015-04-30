# elasticsearch-snapshot-manager
Scala tool for managing elasticsearch snapshots. Creating, deleting, downloading, aggregating, backing up snapshots files across a cluster.

## Status

This is IN PROGRESS, all that is supported right now is coverage for the scenario described in "Overview below"

Rudimentary at this point but works. This little test project involved 3 new things for me: Scala itself!, the elasticsearch java API, coupled with the elastic4s library...

## Overview

This tool is intended to aid with the following scenario:

1. You have a large elasticsearch cluster that spans multiple data-centers
2. You have a "shared filesystem snapshot repository" who's physical location is local to each node and actually NOT on a "shared device" or logical mountpoint (i.e due to (1) above)
3. You need a way to execute the snapshot, then easily collect all the different parts of that snapshot which are located across N nodes across your cluster
4. This tool is intended to automate that process...

## Prerequisites

1. This must be run from a machine that has direct access to port 9300 on all nodes of your elasticsearch cluster
2. You must be able to provide a username/password of a user that exists on all of your elasticsearch nodes, who in-turn has READ access to the local filesystem snapshot directory on each node. (This is used for SSH/SCP operations).. and I will work to support a key based model

## What does this actually do?

1. Queries the cluster for all available snapshot repositories and prompts you to select one.
2. Next it prompts you to pick a snapshot you want to consolidate from that repository
3. Prompts you for the username/password for the SSH user it will connect to all nodes with
4. Proceeds to discover all nodes in the cluster, determine which nodes actually have relevant snapshot metadata/segment files
5. Connects to each relevant node, creates a tar.gz under /tmp (locally on each es node)
6. Finally it downloads all the individual tarballs to your local working dir (specified on command start)
7. You can then run a command like `for i in *.tar.gz; do tar -xvf $i; done` to consolidate them all onto another machine to restore the snapshot somewhere else.

## Usage/Building

* Java: 1.7+
* Scala: 2.11.5 - http://www.scala-lang.org/download/
* Scala SBT: 0.13.8 - http://www.scala-sbt.org/download.html

1. git clone [this project]
2. From the project root: `sbt assembly`
3. Take the resulting jar from `target/scala-[version]/elasticsearch-snapshot-manager-assembly-1.0.jar` and copy it to a machine that has access to all your elasticsearch cluster nodes on port 9300
4. run `java -jar elasticsearch-snapshot-manager-assembly-1.0.jar [esSeedHostname] [pathToEmptyLocalWorkDir]`
