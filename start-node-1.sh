#!/bin/bash
sbt -DTCP_PORT=7000 -DAKKA_PORT=2551 -DDATA_DIR=node1 "runMain io.github.ebezzi.ActorServer"
