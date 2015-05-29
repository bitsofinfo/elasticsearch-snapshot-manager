# elasticsearch-snapshot-manager
Scala tool for managing elasticsearch snapshots. In particular aggregating and backing up snapshots files from fs local-disk snapshot repositories distributed across a large cluster where a "shared" repository location is not avaiable... (i.e. maybe a cluster that spans multiple geographically separated data-centers)

## Status

**beta-code**

## Overview

This tool is intended to aid with the following scenario:

1. You have a large elasticsearch cluster that spans multiple data-centers
2. You have a "shared filesystem snapshot repository" who's physical location is local to each node and actually NOT on a "shared device" or logical mountpoint (i.e due to (1) above), the snapshots reside on local-disk only.
3. You need a way to execute the snapshot, then easily collect all the different parts of that snapshot which are located across N nodes across your cluster
4. This tool is intended to automate that process...

![Alt text](/diagram1.png "Diagram1")

## Prerequisites

1. ElasticSearch 1.5.x
2. This must be run from a machine that has direct access to port 9300 on all nodes of your elasticsearch cluster
3. You must be able to provide a username/password of a user that exists on all of your elasticsearch nodes, who in-turn has READ access to the local filesystem snapshot directory on each node (either directly or via `sudo`). (This is used for SSH/SCP operations).. and yes, work should be done to support a key based model

## What does this actually do?

1. Queries the cluster for all available snapshot repositories and prompts you to select one.
2. Next it prompts you to pick a snapshot you want to consolidate from that repository
3. Prompts you for the username/password for the SSH user it will connect to all nodes with
4. Prompts you if you want all remote SSH commands to be run via `sudo` for the user specified
5. Proceeds to discover all nodes in the cluster, determine which nodes actually have relevant snapshot metadata/segment files
6. For each node, forks and connects to each relevant node in parallel, creates a tar.gz under /tmp (locally on each es node)
7. Finally it downloads all the individual tarballs (in parallel) to your local working dir (specified on command start). Progress is reported every 10 seconds.
8. You can then run a command like `for i in *.tar.gz; do tar -xvf $i; done` to consolidate them all onto another machine to restore the snapshot somewhere else.

## Usage/Building

* Java: 1.7+
* Scala: 2.11.5 - http://www.scala-lang.org/download
* Scala SBT: 0.13.8 - http://www.scala-sbt.org/download.html

1. git clone [this project]
2. From the project root execute: `sbt assembly`
3. Take the resulting jar from:   `target/scala-[version]/elasticsearch-snapshot-manager-assembly-1.0.jar` and copy it to a machine that has access to all your elasticsearch cluster nodes running on port 9300
4. run from a box w/ full access to your es cluster @9300
```
java -jar elasticsearch-snapshot-manager-assembly-1.0.jar [esSeedHostname] [esClusterName] [pathToEmptyLocalWorkDir] [pathToRemoteWorkDir] [minutesToWaitPerDownload]
```

Note the user you specify when prompted must have RW access to the `pathToRemoteWorkDir` specified in the command startup on ALL ES nodes in your cluster.

OPTIONAL JVM arguments you may want to consider:

Tweak your jvm heap size:
```
i.e. .... -Xms1500m -Xmx1500m
```

Tweak the default thread pool, this controls the total number of snapshot data tarballs
that would be concurrently downloaded. You need to balance this w/ the number of availble
cores on the box you are running this from.
```
i.e. .... -Dscala.concurrent.context.numThreads=10 -Dscala.concurrent.context.maxThreads=20
```
## Security

1. This program collects YOUR credentials and uses them to execute several commands over SSH against remote servers that you have access to.
2. You should treat this program no differently than if you were logging into those servers yourself and doing those commands
3. I highly encourage you to review the source code before running if you have any concerns.
4. If you say "yes" to the execute with SUDO option, the SSH session will use forced "psuedo tty" and your password will be echoed from STDIN to the sudo command (via `sudo -S`). The first thing the program does it issue a `export HISTCONTROL=ignorespace` so that all of these echos (prefixed with a space) will not go into `history`.
5. See the source code for what operations are done... basically it copies/tars files out of the Elasticsearch snapshot FS directories into a remote "work dir" and secures those files w/ 600 for the user you specify it to connect against
6. Command injection is likely possible with this tool, but again read 1 and 2 above. If you want to use this tool with your own user account for nefarious purposes, then why not just login and do it yourself?
7. **I highly discourage anyone from exposing access to this tool via another program, web-service or interface that takes un-sanitzed user input**

## Errata

If you get an warning like this, you may want to consider downloading the "Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy Files" appropriate for your JRE/JVM and legal for your locality.

```
13:48:49.816 WARN  DefaultConfig - Disabling high-strength ciphers: cipher strengths apparently limited by JCE policy
```

## Reference

Excellent article on the Elasticsearch snapshot format: https://www.found.no/foundation/elasticsearch-snapshot-and-restore/
