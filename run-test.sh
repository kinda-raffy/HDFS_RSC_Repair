#!/usr/bin/env bash

mvn -Dtest="TestReconstructStripedFile#testRecoverOneDataBlockSmallFile" \
    -DfailIfNoTests=false \
    -Dmaven.surefire.skip=true \
    -Dmaven.surefire.debug \
    test