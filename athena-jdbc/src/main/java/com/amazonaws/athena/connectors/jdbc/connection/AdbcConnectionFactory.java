/*-
 * #%L
 * Amazon Athena JDBC Connector
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
package com.amazonaws.athena.connectors.jdbc.connection;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.driver.jdbc.JdbcDriver;
import org.apache.arrow.adbc.driver.jni.JniDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdbcConnectionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdbcConnectionFactory.class);
    private final DatabaseConnectionConfig databaseConnectionConfig;
    private final Map<String, String> jdbcProperties;
    private final String driverClassName;
    private volatile HikariDataSource hikariDataSource;

    public AdbcConnectionFactory(DatabaseConnectionConfig databaseConnectionConfig, Map<String, String> jdbcProperties) {
        this(databaseConnectionConfig, jdbcProperties, null);
    }

    public AdbcConnectionFactory(DatabaseConnectionConfig databaseConnectionConfig, Map<String, String> jdbcProperties, DatabaseConnectionInfo databaseConnectionInfo) {
        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseConnectionConfig must not be null", new Object[0]);
        this.jdbcProperties = jdbcProperties != null ? new HashMap<String, String>(jdbcProperties) : new HashMap();
        this.driverClassName = databaseConnectionInfo != null ? databaseConnectionInfo.getDriverClassName() : null;
    }

    public AdbcConnection getConnection(CredentialsProvider credentialsProvider, BufferAllocator allocator) throws AdbcException {
        String derivedJdbcString;
        HashMap<String, Object> driverOptions = new HashMap<String, Object>();
        if (credentialsProvider != null) {
            Matcher secretMatcher = GenericJdbcConnectionFactory.SECRET_NAME_PATTERN.matcher(this.databaseConnectionConfig.getJdbcConnectionString());
            String string = secretMatcher.replaceAll(Matcher.quoteReplacement(""));
            derivedJdbcString = string.endsWith("?") ? string.substring(0, string.length() - 1) : string;
            Map<String, String> credentials = credentialsProvider.getCredentialMap();
            if (credentials.containsKey("user")) {
                driverOptions.put(AdbcDriver.PARAM_USERNAME.getKey(), credentials.get("user"));
            }
            if (credentials.containsKey("password")) {
                driverOptions.put(AdbcDriver.PARAM_PASSWORD.getKey(), credentials.get("password"));
            }
        } else {
            derivedJdbcString = this.databaseConnectionConfig.getJdbcConnectionString();
        }
        driverOptions.put(AdbcDriver.PARAM_URI.getKey(), derivedJdbcString);
        for (Map.Entry entry : this.jdbcProperties.entrySet()) {
            driverOptions.put((String)entry.getKey(), entry.getValue());
        }
        LOGGER.info("Creating ADBC connection to: {}", (Object)derivedJdbcString);
        this.loadDriverClass();
        JdbcDriver driver = new JdbcDriver(allocator);
        AdbcDatabase adbcDatabase = driver.open(driverOptions);
        return adbcDatabase.connect();
    }

    public AdbcConnection getNativeConnection(CredentialsProvider credentialsProvider, BufferAllocator allocator, String nativeDriverPath) throws AdbcException {
        return this.getNativeConnection(credentialsProvider, allocator, nativeDriverPath, Collections.emptyMap());
    }

    public AdbcConnection getNativeConnection(CredentialsProvider credentialsProvider, BufferAllocator allocator, String nativeDriverPath, Map<String, String> extraParams) throws AdbcException {
        Object nativeUri;
        Map<String, String> creds;
        String user;
        String password;
        Matcher secretMatcher;
        String jdbcUrl;
        String derivedJdbcString = credentialsProvider != null ? ((jdbcUrl = (secretMatcher = GenericJdbcConnectionFactory.SECRET_NAME_PATTERN.matcher(this.databaseConnectionConfig.getJdbcConnectionString())).replaceAll(Matcher.quoteReplacement(""))).endsWith("?") ? jdbcUrl.substring(0, jdbcUrl.length() - 1) : jdbcUrl) : this.databaseConnectionConfig.getJdbcConnectionString();
        if (derivedJdbcString.contains("jdbc:snowflake://")) {
            nativeUri = derivedJdbcString.replace("jdbc:snowflake://", "");
            if (extraParams != null && !extraParams.isEmpty()) {
                String separator = ((String)nativeUri).contains("?") ? "&" : "?";
                for (Map.Entry<String, String> entry : extraParams.entrySet()) {
                    nativeUri = (String)nativeUri + separator + entry.getKey() + "=" + entry.getValue();
                    separator = "&";
                }
            }
            if (credentialsProvider != null) {
                creds = credentialsProvider.getCredentialMap();
                user = creds.getOrDefault("user", creds.getOrDefault("username", ""));
                password = creds.getOrDefault("password", "");
                nativeUri = user + ":" + password + "@" + (String)nativeUri;
            }
        } else {
            nativeUri = derivedJdbcString.replace("jdbc:postgresql://", "postgresql://");
            if (credentialsProvider != null) {
                creds = credentialsProvider.getCredentialMap();
                user = creds.getOrDefault("user", "");
                password = creds.getOrDefault("password", "");
                nativeUri = ((String)nativeUri).replace("postgresql://", "postgresql://" + user + ":" + password + "@");
            }
        }
        HashMap<String, Object> params = new HashMap<String, Object>();
        params.put("jni.driver", nativeDriverPath);
        params.put("uri", nativeUri);
        LOGGER.info("Creating native ADBC connection via JNI, uri host: {}", ((String)nativeUri).contains("@") ? ((String)nativeUri).substring(((String)nativeUri).indexOf("@") + 1) : nativeUri);
        JniDriver driver = new JniDriver(allocator);
        AdbcDatabase database = driver.open(params);
        return database.connect();
    }

    /*
     * WARNING - Removed try catching itself - possible behaviour change.
     * Enabled force condition propagation
     * Lifted jumps to return sites
     */
    public Connection getJdbcConnection(CredentialsProvider credentialsProvider) throws SQLException {
        String derivedJdbcString;
        Properties connectionProps = new Properties();
        connectionProps.putAll(this.jdbcProperties);
        if (credentialsProvider != null) {
            Matcher secretMatcher = GenericJdbcConnectionFactory.SECRET_NAME_PATTERN.matcher(this.databaseConnectionConfig.getJdbcConnectionString());
            String jdbcUrl = secretMatcher.replaceAll(Matcher.quoteReplacement(""));
            derivedJdbcString = jdbcUrl.endsWith("?") ? jdbcUrl.substring(0, jdbcUrl.length() - 1) : jdbcUrl;
            connectionProps.putAll(credentialsProvider.getCredentialMap());
        } else {
            derivedJdbcString = this.databaseConnectionConfig.getJdbcConnectionString();
        }
        this.loadDriverClass();
        if (this.hikariDataSource != null) return this.hikariDataSource.getConnection();
        Class<AdbcConnectionFactory> clazz = AdbcConnectionFactory.class;
        synchronized (AdbcConnectionFactory.class) {
            if (this.hikariDataSource != null) return this.hikariDataSource.getConnection();
            HikariConfig config = new HikariConfig();
            config.setDriverClassName(this.driverClassName);
            config.setJdbcUrl(derivedJdbcString);
            config.setDataSourceProperties(connectionProps);
            config.setMinimumIdle(1);
            config.setMaximumPoolSize(2);
            this.hikariDataSource = new HikariDataSource(config);
            LOGGER.info("Created HikariCP data source for Arrow JDBC adapter");
            // ** MonitorExit[var4_3] (shouldn't be in output)
            return this.hikariDataSource.getConnection();
        }
    }

    private void loadDriverClass() {
        if (this.driverClassName != null) {
            try {
                Class.forName(this.driverClassName);
            }
            catch (ClassNotFoundException e) {
                throw new RuntimeException("JDBC driver class not found: " + this.driverClassName, e);
            }
        }
    }
}
