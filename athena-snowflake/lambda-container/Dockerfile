# Snowflake ADBC Lambda container image
# Reuses native Go driver libraries from the existing deployed image
FROM 471112813056.dkr.ecr.eu-north-1.amazonaws.com/snowflake-adbc-lambda:latest AS native-libs

# Final Lambda container image
FROM public.ecr.aws/lambda/java:21

# Copy native libraries from existing deployment
COPY --from=native-libs /opt/lib/ /opt/lib/

# Copy the Lambda deployment package
COPY Lambda.zip /tmp/Lambda.zip
RUN cd /var/task && jar xf /tmp/Lambda.zip && rm /tmp/Lambda.zip

# Set the handler
CMD ["com.amazonaws.appflow.connector.snowflake.handler.SnowflakeLambdaHandler"]
