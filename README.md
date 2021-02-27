# Presto ClickHouse Connector

This is a plugin for earlier [PrestoSQL](https://github.com/trinodb/trino/tree/350) that allow you to use ClickHouse Jdbc Connection

## quick started

## compile and installation

### get source code

`git clone -b prestosql-350 https://github.com/wgzhao/presto-clickhouse `


### compile

```shell
mvn clean package assembly:single -DskipTests 
```

After compiling, you can get a zip file in target folder

## installation 

unpack the zip file into your prestosql plugin folder

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