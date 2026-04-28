# ADBC Native PostgreSQL Driver - Lambda Layer Builder
#
# Builds a Lambda layer containing:
# - libadbc_driver_postgresql.so (native ADBC PostgreSQL driver)
# - libadbc_driver_manager.so (ADBC driver manager)
# - libadbc_driver_jni.so (JNI bridge - extracted from Maven JAR)
# - libpq.so.5 (PostgreSQL client library, minimal build)
#
# Usage:
#   docker build -t adbc-layer-builder .
#   docker run --rm -v $(pwd)/output:/output adbc-layer-builder
#
# This produces output/adbc-native-layer.zip ready to upload as a Lambda layer.
# Requires: Lambda runtime java21 (AL2023, glibc 2.34)

FROM public.ecr.aws/amazonlinux/amazonlinux:2023 AS builder

RUN dnf install -y --allowerasing \
    gcc gcc-c++ cmake make git \
    openssl-devel \
    zlib-devel \
    readline-devel \
    curl \
    tar \
    unzip \
    && dnf clean all

# Build PostgreSQL (libpq only) with minimal dependencies - no LDAP, no GSSAPI, no ICU
WORKDIR /build
RUN curl -sL https://ftp.postgresql.org/pub/source/v16.9/postgresql-16.9.tar.gz | tar xz
WORKDIR /build/postgresql-16.9
RUN ./configure \
    --prefix=/opt \
    --without-ldap \
    --without-gssapi \
    --without-icu \
    --without-readline \
    --with-openssl \
    --without-pam \
    --without-systemd \
    && cd src/interfaces/libpq && make -j$(nproc) && make install \
    && cd /build/postgresql-16.9/src/include && make install \
    && cd /build/postgresql-16.9/src/bin/pg_config && make -j$(nproc) && make install

# Build ADBC driver for PostgreSQL
WORKDIR /build
RUN git clone --depth 1 --branch apache-arrow-adbc-20 https://github.com/apache/arrow-adbc.git
WORKDIR /build/arrow-adbc/c
RUN mkdir build && cd build && \
    PKG_CONFIG_PATH=/opt/lib/pkgconfig cmake .. \
    -DCMAKE_INSTALL_PREFIX=/opt \
    -DCMAKE_PREFIX_PATH=/opt \
    -DCMAKE_BUILD_TYPE=Release \
    -DADBC_DRIVER_POSTGRESQL=ON \
    -DADBC_DRIVER_SQLITE=OFF \
    -DADBC_DRIVER_FLIGHTSQL=OFF \
    -DADBC_DRIVER_MANAGER=ON \
    -DADBC_BUILD_TESTS=OFF \
    && make -j$(nproc) \
    && make install \
    && echo "=== Installed files ===" \
    && find /opt -name "*.so*" -type f

# Extract libadbc_driver_jni.so from the published Maven JAR
# This avoids building from source (which needs JDK + Maven + static driver manager)
WORKDIR /build
RUN curl -sL https://repo1.maven.org/maven2/org/apache/arrow/adbc/adbc-driver-jni/0.23.0/adbc-driver-jni-0.23.0.jar -o adbc-driver-jni.jar \
    && unzip -j adbc-driver-jni.jar "adbc_driver_jni/x86_64/libadbc_driver_jni.so" -d /opt/lib/ \
    && echo "=== Extracted JNI library ===" \
    && ls -la /opt/lib/libadbc_driver_jni.so

# Package the layer
FROM public.ecr.aws/amazonlinux/amazonlinux:2023 AS packager

COPY --from=builder /opt/lib/ /layer/lib/
COPY --from=builder /opt/lib64/ /layer/lib/

WORKDIR /layer

RUN dnf install -y zip && \
    mkdir -p /output && \
    echo "=== All files in lib/ ===" && \
    find lib/ -name "*.so*" -type f && \
    echo "=== Checking dependencies ===" && \
    ldd lib/libadbc_driver_postgresql.so || true && \
    echo "=== Checking JNI dependencies ===" && \
    ldd lib/libadbc_driver_jni.so || true && \
    echo "=== Creating layer zip ===" && \
    zip -r /output/adbc-native-layer.zip lib/

CMD ["cp", "/output/adbc-native-layer.zip", "/output/"]
