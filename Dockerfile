FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker .
RUN mkdir -p /spool

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/worker /worker
COPY --from=build --chown=nonroot:nonroot /spool /spool
ENV WORKER_SPOOL_DIR=/spool
VOLUME ["/spool"]
USER nonroot:nonroot
ENTRYPOINT ["/worker"]