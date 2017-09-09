#!/bin/bash
sbt -DTCP_PORT=7001 -DAKKA_PORT=2552 -DDATA_DIR=node2 "runMain io.github.ebezzi.ActorServer"
