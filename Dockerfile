FROM rust:1 AS builder 
WORKDIR /app
COPY . .
RUN cargo build --release


FROM ubuntu:22.04 as runtime
WORKDIR /app
RUN apt-get update && \
    apt-get install -y --quiet mysql-client && \
    rm -rf /var/lib/apt/lists/*
COPY . .
COPY --from=builder /app/target/release/migration /usr/local/bin/migration
RUN ls

CMD ["migration"]