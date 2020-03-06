#!/bin/bash

DIR="$( cd "$( dirname "$0" )" && pwd )"
TOPO="cs535.twitter.topology.TwitterTopology"

function usage {
cat << EOF

	usage: $0 -m -e

	-m ::: mode, parallel and non-parallel
	-e ::: environment, local and cluster

EOF
	exit 1
}

mflag=false
eflag=false

while getopts 'm:e:' flag; do
    case "${flag}" in 
	    m) MODE="${OPTARG}"; mflag=true ;;
	    e) CLUSTER="${OPTARG}"; eflag=true ;;
	    *) usage ;;
    esac
done
shift $((OPTIND-1))

if [[ -z $MODE ]]; then usage; fi

if [[ -z $CLUSTER ]]; then usage; fi

mvn package

storm jar target/storm-twitter-stream-0.0.1-SNAPSHOT-jar-with-dependencies.jar $TOPO $MODE $CLUSTER
