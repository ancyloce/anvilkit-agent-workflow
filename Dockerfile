# anvilkit-agent-workflow: built from this repository alone (the build context
# is the repository root; nothing from the parent checkout is read). The
# generated contract module is an ordinary versioned dependency resolved
# through GOPROXY: pass --build-arg GOPROXY=... (and GONOSUMDB=... for a module
# that is not in the public checksum database yet) to build against a private
# or local module proxy.
FROM golang:1.27.0-alpine@sha256:4c9fe60190a2a3350ddc51de80d0224b8a6698d12bdfc999fee45ea9d6c46dbc AS build
ARG GOPROXY=https://proxy.golang.org,direct
ARG GONOSUMDB=
ENV GOWORK=off GOFLAGS=-mod=readonly CGO_ENABLED=0 GOPROXY=$GOPROXY GONOSUMDB=$GONOSUMDB
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN go build -trimpath -ldflags="-s -w" -o /out/anvilkit-agent-workflow ./cmd/anvilkit-agent-workflow

# Runtime: the binary, the reviewed secret-free configuration file and a
# non-root user. The Temporal and Control addresses, the launch backend, the
# image registry, the build id and the health listener are supplied through
# the allowlisted ANVILKIT_WORKFLOW_* environment overrides; without
# ANVILKIT_WORKFLOW_KUBECONFIG the launcher uses the Pod's ServiceAccount
# identity. No token, kubeconfig or credential is baked into the image.
FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b
COPY --from=build /out/anvilkit-agent-workflow /usr/local/bin/anvilkit-agent-workflow
COPY config.yaml /etc/anvilkit/anvilkit-agent-workflow/config.yaml
ENV ANVILKIT_WORKFLOW_CONFIG=/etc/anvilkit/anvilkit-agent-workflow/config.yaml
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/anvilkit-agent-workflow"]
