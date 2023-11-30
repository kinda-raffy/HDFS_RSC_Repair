sudo rm -rf ../log
mkdir ~/log
# sudo mvn test -Dtest=TestReconstructStripedFile.java -Dmaven.surefire.debug test
sudo mvn test -Dtest=TestReconstructStripedFile#testRecoverOneDataBlockSmallFile -Dmaven.surefire.debug="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005"
