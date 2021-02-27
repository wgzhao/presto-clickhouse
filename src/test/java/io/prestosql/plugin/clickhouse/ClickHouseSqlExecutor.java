/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.clickhouse;

import io.prestosql.testing.sql.SqlExecutor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class ClickHouseSqlExecutor
        implements SqlExecutor
{
    private final String jdbcUrl;
    private final Properties jdbcProperties;

    public ClickHouseSqlExecutor(String jdbcUrl)
    {
        this(jdbcUrl, new Properties());
    }

    public ClickHouseSqlExecutor(String jdbcUrl, Properties jdbcProperties)
    {
        this.jdbcUrl = requireNonNull(jdbcUrl, "jdbcUrl is null");
        this.jdbcProperties = new Properties();
        this.jdbcProperties.putAll(requireNonNull(jdbcProperties, "jdbcProperties is null"));
    }

    @Override
    public void execute(String sql)
    {
        if (sql.startsWith("CREATE TABLE")) {
            sql = sql + " ENGINE=Log";
        }
        try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcProperties);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
            connection.commit();
        }
        catch (SQLException e) {
            throw new RuntimeException("Error executing sql:\n" + sql, e);
        }
    }
}
