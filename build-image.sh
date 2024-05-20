#!/usr/bin/env bash

docker build \
    -t hadoop-build \
    -f dev-support/docker/Dockerfile_aarch64 dev-support/docker