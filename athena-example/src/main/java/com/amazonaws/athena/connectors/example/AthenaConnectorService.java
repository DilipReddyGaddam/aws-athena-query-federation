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

import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.handlers.UserDefinedFunctionHandler;
import com.amazonaws.services.lambda.runtime.Context;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * EC2 Service wrapper for Athena Connector
 * Exposes the CompositeHandler via HTTP instead of Lambda events
 */
public class AthenaConnectorService {
    private static final Logger logger = LoggerFactory.getLogger(AthenaConnectorService.class);
    private static final int PORT = 8080;
    private static final String HANDLER_PATH = "/connector";

    private final CompositeHandler compositeHandler;
    private HttpServer httpServer;

    public AthenaConnectorService() {
        this.compositeHandler = new ExampleCompositeHandler();
    }

    /**
     * Start the HTTP server
     */
    public void start() throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress("0.0.0.0", PORT), 0);
        httpServer.setExecutor(Executors.newFixedThreadPool(10));

        // Register handler for connector requests
        httpServer.createContext(HANDLER_PATH, this::handleRequest);

        // Register health check endpoint
        httpServer.createContext("/health", exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().write("{\"status\":\"UP\"}".getBytes());
            exchange.close();
        });

        httpServer.start();
        logger.info("Athena Connector Service started on port {}", PORT);
    }

    /**
     * Stop the HTTP server gracefully
     */
    public void stop() {
        if (httpServer != null) {
            logger.info("Stopping Athena Connector Service...");
            httpServer.stop(5); // 5 second grace period
        }
    }

    /**
     * Handle incoming HTTP requests
     */
    private void handleRequest(HttpExchange exchange) throws IOException {
        try {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, 0); // Method Not Allowed
                exchange.close();
                return;
            }

            logger.debug("Received request from {}", exchange.getRemoteAddress());
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, 0);

            // Create a mock Lambda context
            Context lambdaContext = new MockLambdaContext();

            // Call the composite handler with request/response streams
            compositeHandler.handleRequest(
                    exchange.getRequestBody(),
                    exchange.getResponseBody(),
                    lambdaContext
            );


            exchange.close();

        } catch (Exception e) {
            logger.error("Error handling request", e);
            exchange.sendResponseHeaders(500, 0);
            exchange.close();
        }
    }

    public static void main(String[] args) {
        try {
            AthenaConnectorService service = new AthenaConnectorService();

            // Add shutdown hook for graceful shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown signal received");
                service.stop();
            }));

            service.start();

            // Keep the service running
            Thread.currentThread().join();

        } catch (Exception e) {
            logger.error("Failed to start Athena Connector Service", e);
            System.exit(1);
        }
    }
}
