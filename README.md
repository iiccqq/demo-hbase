# demo-hbase



mvn dependency:copy-dependencies
tar cvzf lib.tar.gz dependency demo-hbase-0.0.1-SNAPSHOT.jar
tar xvzf lib.tar.gz
cd lib
java -cp "./demo-hbase-0.0.1-SNAPSHOT.jar:./dependency" HbaseTest