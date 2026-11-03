# MinIO server + mc client built from pinned upstream source.
#
# MinIO stopped publishing community container images (Docker Hub denies
# `minio/minio`, quay.io answers "unauthorized"), so we build the exact
# releases we want from GitHub. Both binaries go into one small image used by
# the `minio` and `minio-init` services.
ARG MINIO_RELEASE=RELEASE.2024-10-13T13-34-11Z
ARG MC_RELEASE=RELEASE.2024-10-08T09-37-26Z

FROM golang:1.23.4-alpine AS build
ARG MINIO_RELEASE
ARG MC_RELEASE
RUN apk add --no-cache git
ENV CGO_ENABLED=0
RUN git clone --depth 1 --branch "${MINIO_RELEASE}" https://github.com/minio/minio /src/minio \
 && cd /src/minio && go build -trimpath -ldflags "-s -w" -o /out/minio .
RUN git clone --depth 1 --branch "${MC_RELEASE}" https://github.com/minio/mc /src/mc \
 && cd /src/mc && go build -trimpath -ldflags "-s -w" -o /out/mc .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl \
 && adduser -D -u 1000 minio \
 && mkdir /data && chown minio /data
COPY --from=build /out/minio /out/mc /usr/local/bin/
USER minio
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
