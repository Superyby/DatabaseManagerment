# Build stage
FROM rust:1.75-alpine AS builder

RUN apk add --no-cache musl-dev openssl-dev openssl-libs-static pkgconfig

WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY common ./common
COPY gateway ./gateway
COPY connection-service ./connection-service
COPY query-service ./query-service

# Build all services
RUN cargo build --release

# Gateway image
FROM alpine:3.19 AS gateway
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/target/release/gateway /usr/local/bin/
ENV SERVER_HOST=0.0.0.0
ENV SERVER_PORT=8080
EXPOSE 8080
CMD ["gateway"]

# Connection service image
FROM alpine:3.19 AS connection-service
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/target/release/connection-service /usr/local/bin/
ENV SERVER_HOST=0.0.0.0
ENV SERVER_PORT=8081
EXPOSE 8081
CMD ["connection-service"]

# Query service image
FROM alpine:3.19 AS query-service
RUN apk add --no-cache ca-certificates
COPY --from=builder /app/target/release/query-service /usr/local/bin/
ENV SERVER_HOST=0.0.0.0
ENV SERVER_PORT=8082
EXPOSE 8082
CMD ["query-service"]
