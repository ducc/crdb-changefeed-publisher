FROM rustlang/rust:nightly
WORKDIR .
COPY . .
RUN cargo build --release
ENTRYPOINT ["target/release/crdb-changefeed-publisher"]

