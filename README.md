Similar to the kafka-connect-jdbc connector, but:
* JDBC dialect is always Oracle
* only one view is ever queried
* this will only ever have a SourceConnector, no sinks

Running the DatabaseTest tests requires the correct maven build properties to be set, as well as an Oracle JDBC driver dependency declared in an external profile.

Oracle DB for docker: https://github.com/oracle/docker-images/tree/master/OracleDatabase/SingleInstance

docker run --name <container name> \
-p <host port>:1521 \
-e ORACLE_SID=KCLDB \
-e ORACLE_PDB=KCLPDB \
-e ORACLE_PWD=kminer \
-e ORACLE_CHARACTERSET=AL32UTF8 \
oracle/database:18.3.0-ee