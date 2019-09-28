FROM erlang:21.0 AS erlang
FROM rust:1.38.0 AS rust

FROM debian:9.4

LABEL author="Maxim Molchanov"
LABEL maintainer="m.molchanov@vonmo.com"
LABEL version="1.0"
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
    ca-certificates \
    python-dev \
    python-pip \
    python-setuptools \
    make \
    cmake \
    locales \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists /var/cache/apt \
    && find /usr/local -name examples | xargs rm -rf

RUN echo "LC_ALL=en_US.UTF-8" >> /etc/environment
RUN echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen
RUN echo "LANG=en_US.UTF-8" > /etc/locale.conf
RUN locale-gen en_US.UTF-8

ADD ./docker-entry.sh /docker-entry.sh

CMD ["/docker-entry.sh"]
