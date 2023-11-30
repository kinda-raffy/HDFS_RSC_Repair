debug() {
    local file=$1
    local method=${2:-"*"}
    sudo mvn test -Dtest="${file}#${method}" -Dmaven.surefire.debug="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005"
}
