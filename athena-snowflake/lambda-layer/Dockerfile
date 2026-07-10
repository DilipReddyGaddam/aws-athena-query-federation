# Lambda Layer build for ADBC native driver (UNUSED - container image approach used instead)
# Kept for reference. The Go driver (74MB) exceeds Lambda zip+layer limits (262MB total).
FROM golang:1.22 AS builder

RUN apt-get update && apt-get install -y cmake git

# Build ADBC Snowflake driver
RUN git clone --depth 1 --branch apache-arrow-adbc-20 https://github.com/apache/arrow-adbc.git /adbc
WORKDIR /adbc/go/adbc
RUN CGO_ENABLED=1 go build -buildmode=c-shared \
    -ldflags="-s -w" \
    -o /opt/lib/libadbc_driver_snowflake.so \
    ./pkg/snowflake

# Build driver manager
WORKDIR /adbc/c/driver_manager
RUN mkdir build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && make -j$(nproc)
RUN cp build/libadbc_driver_manager.so /opt/lib/

# Package as Lambda layer
FROM amazonlinux:2023 AS packager
COPY --from=builder /opt/lib/ /opt/lib/
RUN cd /opt && zip -r /layer.zip lib/
