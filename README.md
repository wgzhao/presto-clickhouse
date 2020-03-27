# Presto ClickHouse Connector

This is a plugin for Presto that allow you to use ClickHouse Jdbc Connection

[![Presto-Connectors Member](https://img.shields.io/badge/presto--connectors-member-green.svg)](http://presto-connectors.ml)

## Connection Configuration

Create new properties file inside etc/catalog dir:

    connector.name=clickhouse
    # connection-url must me the URL to access ClickHouse via JDBC. It can be different depending on your environment.
    # Another example of the URL would be jdbc:ClickHousehin:@//ip:port/database. For more information, please go to the JDBC driver docs
    connection-url=jdbc:clickhouse://ip:port/database
    connection-user=myuser
    connection-password=

Create a dir inside plugin dir called ClickHouse. To make it easier you could copy mysql dir to ClickHouse and remove the mysql-connector and prestodb-mysql jars. Finally put the prestodb-ClickHouse in plugin/ClickHouse folder. Here is the sptes:

    cd $PRESTODB_HOME
    mkdir plugin/clickhouse
    mv /home/Downloads/presto-ClickHouse*.jar plugin/clickhouse

## Building Presto ClickHouse JDBC Plugin

    mvn clean install
    
## Building ClickHouse Driver
ClickHouse Driver is not available in common repositories, so you will need to download it from ClickHouse and install manually in your repository.
