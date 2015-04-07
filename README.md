# elasticsearch-snapshot-manager
Scala tool for managing elasticsearch snapshots. Creating, deleting, downloading, aggregating, backing up snapshots files across a cluster.

## Overview

This tool is intended to aid with the following scenario:

1. You have a large elasticsearch cluster that spans multiple data-centers
2. You have a "shared filesystem snapshot repository" who's physical location is local to each node and NOT on a shared device or logical mountpoint (i.e due to (1) above)
3. You need a way to execute the snapshot, then easily collect all the different parts of that snapshot which are located across N nodes across your cluster 
4. This tool is intended to automate that process...

## Coming soon... in progress
