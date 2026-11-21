# Dynamic glibc build: glibc's allocator and memcpy/memchr are much faster under 8 threads than
# musl's, and the runtime stage ships the same libc the binary was linked against (bookworm).
FROM rust:1.93.1-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src src
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /build/target/release/shopping-sessions /shopping-sessions
USER 1000:1000
ENTRYPOINT ["/shopping-sessions"]
