FROM debian:bookworm AS builder

ARG GO_VERSION=1.24.2
ARG RUST_VERSION=1.86.0
ARG CBINDGEN_VERSION=0.28.0
ARG ZIG_VERSION=0.15.2
ARG TARGETARCH

RUN apt-get update && apt-get install -y \
    build-essential \
    ca-certificates \
    curl \
    git \
    kcov \
    libzmq5-dev \
    libjemalloc-dev \
    pkg-config \
    wget \
    xz-utils \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

RUN if [ "$TARGETARCH" = "arm64" ]; then \
        echo "Building for ARM64 architecture"; \
        export GO_ARCH="arm64"; \
        export ZIG_ARCH="aarch64"; \
    else \
        echo "Building for x86_64 architecture"; \
        export GO_ARCH="amd64"; \
        export ZIG_ARCH="x86_64"; \
    fi \
    && echo "GO_ARCH=$GO_ARCH" >> /etc/environment \
    && echo "ZIG_ARCH=$ZIG_ARCH" >> /etc/environment

RUN . /etc/environment && \
    curl -sSL "https://golang.org/dl/go${GO_VERSION}.linux-${GO_ARCH}.tar.gz" | tar -C /usr/local -xz
ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOPATH="/go"
ENV PATH="${GOPATH}/bin:${PATH}"

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain ${RUST_VERSION}
ENV PATH="/root/.cargo/bin:${PATH}"

RUN cargo install --version ${CBINDGEN_VERSION} cbindgen

RUN . /etc/environment && \
    wget -O zig.tar.xz "https://ziglang.org/download/${ZIG_VERSION}/zig-linux-${ZIG_ARCH}-${ZIG_VERSION}.tar.xz" \
    && mkdir -p /usr/local/zig \
    && tar -xf zig.tar.xz -C /usr/local/zig --strip-components=1 \
    && rm zig.tar.xz
ENV PATH="/usr/local/zig:${PATH}"

WORKDIR /app

COPY . .

RUN zig build -Dcpu=native -Doptimize=ReleaseFast --summary all --verbose go 

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    libzmq5 \
    libjemalloc2 \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH
LABEL architecture="${TARGETARCH}"

WORKDIR /app

COPY --from=builder /app/zig-out/bin/gopg /usr/local/bin/
COPY --from=builder /app/zig-out /app/zig-out

ENTRYPOINT ["ls", "/app/zig-out/"]
