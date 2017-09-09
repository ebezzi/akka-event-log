#!/bin/bash
sbt -DTCP_PORT=7000 -DAKKA_PORT=2559 -DDATA_DIR=node1 "runMain io.github.ebezzi.Producer"
