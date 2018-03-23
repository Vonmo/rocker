FROM elzor/erlang:20.3 AS erlang
FROM rust:1.24 AS rust

FROM debian:9.4

COPY --from=erlang /erl /erl
ENV PATH=/erl/bin:$PATH

COPY --from=rust /usr/local/rustup /usr/local/rustup
COPY --from=rust /usr/local/cargo /usr/local/cargo

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN apt-get update && apt-get install --no-install-recommends -y \
    apt-transport-https \
    wget \
    curl \
    build-essential \
    git \
    openssl \
    libssl-dev \
    libncurses5 \
    libncurses5-dev \
    xsltproc \
    automake \
    autoconf \
    clang \
    libclang-dev \
    procps \
    ca-certificates

ADD ./docker-entry.sh /docker-entry.sh

CMD ["/docker-entry.sh"]
