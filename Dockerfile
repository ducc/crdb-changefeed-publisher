FROM rustlang/rust:nightly

WORKDIR .

COPY . .

RUN cargo build 

#CMD ["/usr/local/cargo/bin/crdb-changefeed-publisher"]
ENTRYPOINT ["target/debug/crdb-changefeed-publisher"]
