# Trino ClickHouse Connector

This is a plugin for Trino that allow you to use ClickHouse Jdbc Connection

[![Trino-Connectors Member](https://img.shields.io/badge/presto--connectors-member-green.svg)](http://presto-connectors.ml)

## compile and installation

### get source code

`git clone https://github.com/wgzhao/presto-clickhouse`
   
### Switching to the appropriate branch

Switch to the branch corresponding to your trino or prestosql version: 
for example, if you have trino version 352, switch to `trino-352`, if you have prestosql version 348,
switch to `prestosql-348`

```shell
cd presto-clickhouse
git checkout trino-352
```

### compile

```shell
mvn clean package assembly:single -DskipTests 
```

After compiling, you can get a zip file in target folder

## installation 

unpack the zip file into your trino/prestosql plugin folder

```shell
unzip target/trino-clickhouse-<version>.zip -d /usr/lib/trino/plugin 
```

## Connection Configuration

Create new properties file inside etc/catalog dir:

```ini
connector.name=clickhouse
connection-url=jdbc:clickhouse://ip:port/database
connection-user=myuser
connection-password=
```

your can get detailed in the [documentation](clickhouse.md).