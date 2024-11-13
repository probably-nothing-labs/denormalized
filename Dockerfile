# This file builds an image that runs Kakfa and the emit_measurments.rs scripts for generating fake data
# 
# docker build -t emgeee/kafka_emit_measurements:latest .
#

# Stage 1: Build the Rust application
FROM rust:1.81.0-slim-bookworm AS builder
WORKDIR /usr/src/app

# Install build dependencies and zig
RUN apt-get update && apt-get install -y \
    cmake \
    g++ \
    libssl-dev \
    pkg-config \
    wget \
    xz-utils \
    && wget https://ziglang.org/download/0.13.0/zig-linux-x86_64-0.13.0.tar.xz \
    && tar -xf zig-linux-x86_64-0.13.0.tar.xz \
    && mv zig-linux-x86_64-0.13.0 /usr/local/zig \
    && rm zig-linux-x86_64-0.13.0.tar.xz \
    && rm -rf /var/lib/apt/lists/*

# Add zig to PATH
ENV PATH="/usr/local/zig/:$PATH"

# Install cargo-zigbuild
RUN cargo install --locked cargo-zigbuild && \
    rustup target add x86_64-unknown-linux-musl

# Copy and build
COPY . .
RUN cargo zigbuild --target x86_64-unknown-linux-musl --release --example emit_measurements && \
    cp target/x86_64-unknown-linux-musl/release/examples/emit_measurements /tmp/ && \
    rm -rf /usr/src/app/*

# Stage 2: Final image with Kafka and Rust binary
FROM confluentinc/cp-kafka:7.5.1
USER root

# Install minimal dependencies
RUN yum update -y && \
    yum install -y openssl-devel && \
    yum clean all && \
    rm -rf /var/cache/yum

# Copy only the binary from builder stage
COPY --from=builder /tmp/emit_measurements /usr/local/bin/

# Create startup script
COPY <<EOF /startup.sh
#!/bin/bash
# Generate Cluster ID if not provided
CLUSTER_ID=\${CLUSTER_ID:-\$(kafka-storage random-uuid)}
echo "Using Cluster ID: \$CLUSTER_ID"
export CLUSTER_ID

# Format storage directory
echo "Formatting storage directory..."
kafka-storage format -t \$CLUSTER_ID -c /etc/kafka/kraft/server.properties

# Start Kafka with KRaft
/etc/confluent/docker/run &

# Wait for Kafka to be ready
until kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; do
  echo "Waiting for Kafka to be ready..."
  sleep 5
done

# Create topics with 1GB retention
echo "Creating temperature topic..."
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic temperature --partitions 1 --replication-factor 1 --config retention.bytes=1073741824

echo "Creating humidity topic..."
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic humidity --partitions 1 --replication-factor 1 --config retention.bytes=1073741824

# Start the Rust application
/usr/local/bin/emit_measurements
EOF

RUN chmod +x /startup.sh

# Kafka configuration
ENV KAFKA_NODE_ID=1 \
    KAFKA_PROCESS_ROLES=broker,controller \
    KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1

# Expose Kafka port
EXPOSE 9092

CMD ["/startup.sh"]
