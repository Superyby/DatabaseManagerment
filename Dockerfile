# ============================================
# 阶段 1: 依赖缓存层（大幅加速重复构建）
# ============================================
FROM rust:1.75-alpine AS deps

RUN apk add --no-cache musl-dev openssl-dev openssl-libs-static pkgconfig

WORKDIR /app

# 先只复制 Cargo 文件，构建依赖缓存
COPY Cargo.toml Cargo.lock ./
COPY common/Cargo.toml ./common/
COPY gateway/Cargo.toml ./gateway/
COPY connection-service/Cargo.toml ./connection-service/
COPY query-service/Cargo.toml ./query-service/
COPY ai-service/Cargo.toml ./ai-service/

# 创建空的 src 文件让 cargo 可以构建依赖
RUN mkdir -p common/src gateway/src connection-service/src query-service/src ai-service/src && \
    echo "pub fn dummy() {}" > common/src/lib.rs && \
    echo "fn main() {}" > gateway/src/main.rs && \
    echo "fn main() {}" > connection-service/src/main.rs && \
    echo "fn main() {}" > query-service/src/main.rs && \
    echo "fn main() {}" > ai-service/src/main.rs

# 预编译依赖（这一层会被缓存）
RUN cargo build --release && rm -rf src target/release/deps/gateway* target/release/deps/connection* target/release/deps/query* target/release/deps/ai* target/release/deps/common*

# ============================================
# 阶段 2: 构建应用
# ============================================
FROM deps AS builder

# 复制真正的源代码
COPY common/src ./common/src
COPY gateway/src ./gateway/src
COPY connection-service/src ./connection-service/src
COPY query-service/src ./query-service/src
COPY ai-service/src ./ai-service/src

# 构建所有服务（依赖已缓存，只编译业务代码）
RUN cargo build --release

# 压缩二进制文件（可选，减小体积）
RUN apk add --no-cache upx && \
    upx --best --lzma target/release/gateway || true && \
    upx --best --lzma target/release/connection-service || true && \
    upx --best --lzma target/release/query-service || true && \
    upx --best --lzma target/release/ai-service || true

# ============================================
# 阶段 3: 最终镜像（使用 scratch 最小化）
# ============================================

# Gateway
FROM scratch AS gateway
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/target/release/gateway /gateway
ENV SERVER_HOST=0.0.0.0
ENV SERVER_PORT=8080
EXPOSE 8080
ENTRYPOINT ["/gateway"]

# Connection Service
FROM scratch AS connection-service
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/target/release/connection-service /connection-service
ENV SERVER_HOST=0.0.0.0
ENV SERVER_PORT=8081
EXPOSE 8081
ENTRYPOINT ["/connection-service"]

# Query Service
FROM scratch AS query-service
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/target/release/query-service /query-service
ENV SERVER_HOST=0.0.0.0
ENV SERVER_PORT=8082
EXPOSE 8082
ENTRYPOINT ["/query-service"]

# AI Service
FROM scratch AS ai-service
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/target/release/ai-service /ai-service
ENV SERVER_HOST=0.0.0.0
ENV SERVER_PORT=8083
EXPOSE 8083
ENTRYPOINT ["/ai-service"]

# ============================================
# 可选：全合一镜像（单容器运行所有服务）
# ============================================
FROM alpine:3.19 AS all-in-one
RUN apk add --no-cache ca-certificates supervisor
COPY --from=builder /app/target/release/gateway /usr/local/bin/
COPY --from=builder /app/target/release/connection-service /usr/local/bin/
COPY --from=builder /app/target/release/query-service /usr/local/bin/
COPY --from=builder /app/target/release/ai-service /usr/local/bin/

# Supervisor 配置
RUN mkdir -p /etc/supervisor.d
COPY <<EOF /etc/supervisor.d/services.ini
[supervisord]
nodaemon=true
logfile=/dev/null
logfile_maxbytes=0

[program:connection-service]
command=/usr/local/bin/connection-service
environment=SERVER_PORT=8081,SERVER_HOST=0.0.0.0
stdout_logfile=/dev/stdout
stdout_logfile_maxbytes=0
stderr_logfile=/dev/stderr
stderr_logfile_maxbytes=0

[program:query-service]
command=/usr/local/bin/query-service
environment=SERVER_PORT=8082,SERVER_HOST=0.0.0.0,CONNECTION_SERVICE_URL=http://127.0.0.1:8081
stdout_logfile=/dev/stdout
stdout_logfile_maxbytes=0
stderr_logfile=/dev/stderr
stderr_logfile_maxbytes=0

[program:ai-service]
command=/usr/local/bin/ai-service
environment=SERVER_PORT=8083,SERVER_HOST=0.0.0.0,CONNECTION_SERVICE_URL=http://127.0.0.1:8081,QUERY_SERVICE_URL=http://127.0.0.1:8082
stdout_logfile=/dev/stdout
stdout_logfile_maxbytes=0
stderr_logfile=/dev/stderr
stderr_logfile_maxbytes=0

[program:gateway]
command=/usr/local/bin/gateway
environment=SERVER_PORT=8080,SERVER_HOST=0.0.0.0,CONNECTION_SERVICE_URL=http://127.0.0.1:8081,QUERY_SERVICE_URL=http://127.0.0.1:8082,AI_SERVICE_URL=http://127.0.0.1:8083
stdout_logfile=/dev/stdout
stdout_logfile_maxbytes=0
stderr_logfile=/dev/stderr
stderr_logfile_maxbytes=0
EOF

EXPOSE 8080 8081 8082 8083
CMD ["supervisord", "-c", "/etc/supervisor.d/services.ini"]
