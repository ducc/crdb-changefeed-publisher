FROM rustlang/rust:nightly as builder
WORKDIR .
COPY cli.yml .
COPY src/* src/
COPY Cargo.toml .
COPY Cargo.lock .
RUN cargo build --release

FROM ubuntu
RUN apt-get update && apt-get install -y libssl-dev ca-certificates
COPY --from=builder /target/release/crdb-changefeed-publisher .
ENTRYPOINT ["/crdb-changefeed-publisher"]

