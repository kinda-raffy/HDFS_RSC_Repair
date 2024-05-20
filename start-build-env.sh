#!/usr/bin/env bash
set -e
docker run --name hadoop-build --rm -it \
    -v "./:/home/hadoop/hdfs-trace-repair" \
    -v "${HOME}/.m2:/home/hadoop/.m2"  \
    -p "5005:5005" \
    -h hadoop-build \
    --network hadoop \
    hadoop-build