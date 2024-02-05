FROM erlang:26.2.1 AS erlang
FROM rust:1.75.0-bookworm AS rust
FROM debian:11.8

LABEL author="Maxim Molchanov"
LABEL maintainer="m.molchanov@vonmo.com"
LABEL version="2.0"
LABEL description="Base docker image for Rocker"

ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY --from=erlang /usr/local/lib/erlang /usr/local/lib/erlang
ENV PATH=/usr/local/lib/erlang/bin:$PATH

COPY --from=rust /usr/local/rustup /usr/local/rustup
COPY --from=rust /usr/local/cargo /usr/local/cargo

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN apt-get update && apt-get install --no-install-recommends -y \
    apt-transport-https \
    ca-certificates \
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
    python3-full \
    python3-pip \
    make \
    cmake \
    locales \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists /var/cache/apt \
    && find /usr/local -name examples | xargs rm -rf

RUN echo "LC_ALL=en_US.UTF-8" >> /etc/environment \
 && echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen \
 && echo "LANG=en_US.UTF-8" > /etc/locale.conf \
 && locale-gen en_US.UTF-8

ADD ./docker-entry.sh /docker-entry.sh

CMD ["/docker-entry.sh"]
