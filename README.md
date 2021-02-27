# PrestoSQL ClickHouse Connector

This is a plugin for [PrestoSQL](https://trino.io) that allow you to use ClickHouse Jdbc Connection

[![Presto-Connectors Member](https://img.shields.io/badge/presto--connectors-member-green.svg)](https://trino.io)

## quick started

You can execute the following instructions to get `presto-clickhouse` plugin

```shell
wget -O /tmp/presto-clickhouse-352.zip \
 https://github.com/wgzhao/presto-clickhouse/releases/download/350/presto-clickhouse-350.zip
unzip -q -o /tmp/presto-clickhouse-352.zip -d /usr/lib/presto/plugin
```

Once this is done, you can activate the clickhouse plugin by executing `/etc/init.d/trino restart`

## compile and installation

```shell
git clone -b prestosql-350 https://github.com/wgzhao/presto-clickhouse`
mvn clean package assembly:single -DskipTests 
```

After compiling, you can get a zip file in target folder,
unpack the zip file into your presto plugin folder

```shell
unzip target/presto-clickhouse-350.zip -d /usr/lib/presto/plugin 
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