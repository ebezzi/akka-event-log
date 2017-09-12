#!/bin/bash
mkdir -p node$1
TCP_PORT=$((7000+$1))
AKKA_PORT=$((2550+$1))
DATA_DIR=node$1
sbt -DTCP_PORT=$TCP_PORT -DAKKA_PORT=$AKKA_PORT -DDATA_DIR=$DATA_DIR "runMain io.github.ebezzi.ActorServer"
