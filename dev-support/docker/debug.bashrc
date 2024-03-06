debug() {
    local file=$1
    local method=${2:-"*"}

    sudo mvn test -Dtest="${file}#${method}" \
                  -DfailIfNoTests=false \
                  -Dmaven.surefire.skip=true \
                  -Dmaven.surefire.debug="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005" \
                  -DskipTests=false
}

alias trdebug="debug TestReconstructStripedFile testRecoverOneDataBlockSmallFile"
