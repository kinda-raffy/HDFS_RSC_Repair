debug() {
    local file=$1
    local method=${2:-"*"}

    mvn test -Dtest="${file}#${method}" \
                  -DfailIfNoTests=false \
                  -Dmaven.surefire.skip=true \
                  -Dmaven.surefire.debug="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005" \
                  -DskipTests=false
}

alias trdebug="debug TestReconstructStripedFile testRecoverOneDataBlockSmallFile"