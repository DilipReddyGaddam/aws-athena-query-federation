/*-
 * #%L
 * athena-example
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
package com.amazonaws.athena.connectors.example;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mock Lambda Context for EC2 execution
 */
public class MockLambdaContext implements Context {
    private static final Logger logger = LoggerFactory.getLogger(MockLambdaContext.class);
    private static final String REQUEST_ID = "ec2-request-" + System.currentTimeMillis();

    @Override
    public String getAwsRequestId() {
        return REQUEST_ID;
    }

    @Override
    public String getLogGroupName() {
        return "/aws/athena-connector/ec2";
    }

    @Override
    public String getLogStreamName() {
        return "connector-stream";
    }

    @Override
    public String getFunctionName() {
        return "athena-connector-ec2";
    }

    @Override
    public String getFunctionVersion() {
        return "$LATEST";
    }

    @Override
    public String getInvokedFunctionArn() {
        return "arn:aws:lambda:us-east-1:123456789012:function:athena-connector-ec2";
    }

    @Override
    public int getMemoryLimitInMB() {
        return 3008; // Default Lambda memory
    }

    @Override
    public int getRemainingTimeInMillis() {
        return 900000; // 15 minutes
    }

    @Override
    public LambdaLogger getLogger() {
        return new LambdaLogger() {
            @Override
            public void log(String message) {
                logger.info(message);
            }

            @Override
            public void log(byte[] message) {
                logger.info(new String(message));
            }
        };
    }

    @Override
    public ClientContext getClientContext() {
        return null;
    }

    @Override
    public CognitoIdentity getIdentity() {
        return null;
    }
}

