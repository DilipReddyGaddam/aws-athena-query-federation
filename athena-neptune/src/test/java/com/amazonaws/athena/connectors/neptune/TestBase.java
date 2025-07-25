/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.neptune;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;

import java.util.Collections;
/**
 * This class is a Base class for other Test classes
 */
public class TestBase {
    protected static final FederatedIdentity IDENTITY = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
    protected static final String QUERY_ID = "query_id-" + System.currentTimeMillis();
    protected static final String PARTITION_ID = "partition_id";
    protected static final String DEFAULT_CATALOG = "default";
    protected static final String TEST_TABLE = "default";
    protected static final String DEFAULT_SCHEMA = "deafult-database";
    protected static final String CONNECTION_STRING = "connectionString";
    protected static final TableName TABLE_NAME = new TableName(DEFAULT_SCHEMA, TEST_TABLE);
}
