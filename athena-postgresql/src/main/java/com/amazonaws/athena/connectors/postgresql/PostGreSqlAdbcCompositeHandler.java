/*-
 * #%L
 * athena-postgresql
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.postgresql;

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

/**
 * ADBC-based composite handler for PostgreSQL that uses native Arrow columnar data retrieval.
 * Uses the existing {@link PostGreSqlMetadataHandler} for metadata and the new
 * {@link PostGreSqlAdbcRecordHandler} for data retrieval via ADBC.
 *
 * <p>This can be deployed side-by-side with the JDBC-based {@link PostGreSqlCompositeHandler}
 * for performance comparison.</p>
 */
public class PostGreSqlAdbcCompositeHandler
        extends CompositeHandler
{
    public PostGreSqlAdbcCompositeHandler()
    {
        super(new PostGreSqlMetadataHandler(new PostGreSqlEnvironmentProperties().createEnvironment()),
                new PostGreSqlAdbcRecordHandler(new PostGreSqlEnvironmentProperties().createEnvironment()));
    }
}
