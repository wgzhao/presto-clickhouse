# Trino ClickHouse Connector

This is a plugin for Trino that allow you to use ClickHouse Jdbc Connection

[![Trino-Connectors Member](https://img.shields.io/badge/trino--connectors-member-green.svg)](https://trino.io)

## quick started

You can execute the following instructions to get `trino-clickhouse` plugin

```shell
wget -O /tmp/trino-clickhouse-352.zip \
 https://github.com/wgzhao/presto-clickhouse/releases/download/352/trino-clickhouse-352.zip
unzip -q -o /tmp/trino-clickhouse-352.zip -d /usr/lib/trino/plugin
```

Once this is done, you can activate the clickhouse plugin by executing `/etc/init.d/trino restart`

## compile and installation

```shell
git clone -b trino-352 https://github.com/wgzhao/presto-clickhouse`
mvn clean package assembly:single -DskipTests 
```

After compiling, you can get a zip file in target folder,
unpack the zip file into your trino plugin folder

```shell
unzip target/trino-clickhouse-352.zip -d /usr/lib/trino/plugin 
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