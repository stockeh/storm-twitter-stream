#!/bin/bash

DIR="$( cd "$( dirname "$0" )" && pwd )"

TOPO="cs535.twitter.topology.TwitterTopology"

mvn package

storm jar target/storm-twitter-stream-0.0.1-SNAPSHOT-jar-with-dependencies.jar $TOPO
