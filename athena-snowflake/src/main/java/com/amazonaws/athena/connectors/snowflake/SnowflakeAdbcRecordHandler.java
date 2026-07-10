/*-
 * #%L
 * athena-snowflake
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
/*
 * Decompiled with CFR 0.152.
 */
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.substrait.SubstraitSqlUtils;
import com.amazonaws.athena.connectors.jdbc.connection.AdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.manager.AdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.athena.connectors.snowflake.SnowflakeConstants;
import com.amazonaws.athena.connectors.snowflake.SnowflakeEnvironmentProperties;
import com.amazonaws.athena.connectors.snowflake.SnowflakeFederationExpressionParser;
import com.amazonaws.athena.connectors.snowflake.SnowflakeQueryStringBuilder;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.SnowflakeSqlDialect;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

public class SnowflakeAdbcRecordHandler
extends AdbcRecordHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeAdbcRecordHandler.class);
    private static final String NATIVE_DRIVER_PATH = "/opt/lib/libadbc_driver_snowflake.so";
    private final JdbcSplitQueryBuilder jdbcSplitQueryBuilder;

    public SnowflakeAdbcRecordHandler(Map<String, String> configOptions) {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv("snowflake", configOptions), configOptions);
    }

    public SnowflakeAdbcRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, Map<String, String> configOptions) {
        this(databaseConnectionConfig, S3Client.create(), SecretsManagerClient.create(), AthenaClient.create(), new AdbcConnectionFactory(databaseConnectionConfig, SnowflakeEnvironmentProperties.getSnowFlakeParameter(SnowflakeConstants.JDBC_PROPERTIES, configOptions), new DatabaseConnectionInfo("com.snowflake.client.jdbc.SnowflakeDriver", 1025)), new SnowflakeQueryStringBuilder("\"", new SnowflakeFederationExpressionParser("\"")), configOptions);
    }

    @VisibleForTesting
    public SnowflakeAdbcRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, S3Client amazonS3, SecretsManagerClient secretsManager, AthenaClient athena, AdbcConnectionFactory adbcConnectionFactory, JdbcSplitQueryBuilder jdbcSplitQueryBuilder, Map<String, String> configOptions) {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig, adbcConnectionFactory, configOptions);
        this.jdbcSplitQueryBuilder = Validate.notNull(jdbcSplitQueryBuilder, "query builder must not be null", new Object[0]);
    }

    @Override
    protected String getNativeDriverPath() {
        if (new File(NATIVE_DRIVER_PATH).exists()) {
            LOGGER.info("Native ADBC Snowflake Go driver found at {}", (Object)NATIVE_DRIVER_PATH);
            return NATIVE_DRIVER_PATH;
        }
        LOGGER.info("Native ADBC driver not found at {}, falling back to JDBC adapter", (Object)NATIVE_DRIVER_PATH);
        return null;
    }

    @Override
    protected Map<String, String> getNativeConnectionParams() {
        String schema;
        HashMap<String, String> params = new HashMap<String, String>();
        String database = (String)this.configOptions.get("DATABASE");
        if (database == null) {
            database = (String)this.configOptions.get("database");
        }
        if (database != null) {
            params.put("db", database);
        }
        if ((schema = (String)this.configOptions.get("SCHEMA")) == null) {
            schema = (String)this.configOptions.get("schema");
        }
        if (schema != null) {
            params.put("schema", schema);
        }
        return params;
    }

    @Override
    protected String buildSqlForAdbc(ReadRecordsRequest request) {
        if (request.getConstraints().isQueryPassThrough()) {
            return request.getConstraints().getQueryPassthroughArguments().get("QUERY");
        }
        if (request.getConstraints().getQueryPlan() != null && request.getConstraints().getQueryPlan().getSubstraitPlan() != null) {
            String plan = request.getConstraints().getQueryPlan().getSubstraitPlan();
            SqlNode sqlNode = SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(plan, SnowflakeSqlDialect.DEFAULT);
            String sql = sqlNode.toSqlString(SnowflakeSqlDialect.DEFAULT).getSql();
            String database = (String)this.configOptions.get("DATABASE");
            if (database == null) {
                database = (String)this.configOptions.get("database");
            }
            if (database != null && !((String)sql).contains(database + ".") && !((String)sql).contains("\"" + database + "\"")) {
                String schemaName = request.getTableName().getSchemaName();
                String tableName = request.getTableName().getTableName();
                String qualifiedTable = "\"" + database + "\".\"" + schemaName + "\".\"" + tableName + "\"";
                sql = ((String)sql).replace("\"" + schemaName + "\".\"" + tableName + "\"", qualifiedTable);
            }
            String partitionSchemaName = request.getSplit().getProperty("partition_schema_name");
            String partitionName = request.getSplit().getProperty("partition_name");
            if ("*".equals(partitionSchemaName) && partitionName != null && !"*".equals(partitionName)) {
                sql = ((String)sql).toUpperCase().contains("WHERE") ? (String)sql + " AND " + partitionName : (String)sql + " WHERE " + partitionName;
            }
            LOGGER.info("Snowflake ADBC QueryPlan SQL: {}", sql);
            return sql;
        }
        Set<String> partitionCols = request.getSplit().getProperties().keySet();
        StringBuilder sb = new StringBuilder("SELECT ");
        boolean first = true;
        for (Field field : request.getSchema().getFields()) {
            if (partitionCols.contains(field.getName())) continue;
            if (!first) {
                sb.append(", ");
            }
            sb.append("\"").append(field.getName()).append("\"");
            first = false;
        }
        sb.append(" FROM ");
        Object database = (String)this.configOptions.get("DATABASE");
        if (database == null) {
            database = (String)this.configOptions.get("database");
        }
        if (database != null) {
            sb.append("\"").append((String)database).append("\".");
        }
        sb.append("\"").append(request.getTableName().getSchemaName()).append("\".\"").append(request.getTableName().getTableName()).append("\"");
        LOGGER.info("Snowflake ADBC SQL: {}", (Object)sb);
        return sb.toString();
    }
}
