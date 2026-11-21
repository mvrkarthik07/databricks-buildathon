FROM rust:1.93.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add x86_64-unknown-linux-musl
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src src
ENV RUSTFLAGS="-C target-feature=+crt-static"
RUN cargo build --release --locked --target x86_64-unknown-linux-musl

FROM scratch
COPY --from=build /build/target/x86_64-unknown-linux-musl/release/shopping-sessions /shopping-sessions
USER 1000:1000
ENTRYPOINT ["/shopping-sessions"]
