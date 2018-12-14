Similar to the kafka-connect-jdbc connector, but:
* JDBC dialect is always Oracle
* only one view is ever queried
* this will only ever have a SourceConnector, no sinks

Running the DatabaseTest requires an appropriate Oracle Database docker image as well as an Oracle JDBC driver available in your Maven repository.

Oracle DB for docker: https://github.com/oracle/docker-images/tree/master/OracleDatabase/SingleInstance

TODO:
* get docker-maven-plugin to reuse a host volume if available for the database files, even if docker isn't running on localhost (e.g, docker-machine)
* ensure the startup/setup volumes are mounting
* get the docker-maven-plugin mapped db port into the filtered test resources