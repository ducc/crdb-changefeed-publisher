FROM rustlang/rust:nightly
WORKDIR .
COPY . .
RUN cargo build --release

FROM alpine:3.7
ADD target/release/crdb-changefeed-publisher .
ENTRYPOINT ["crdb-changefeed-publisher"]
