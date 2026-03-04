# syntax=docker/dockerfile:1.7

FROM golang:1.23-alpine AS build
WORKDIR /src

RUN apk add --no-cache ca-certificates git

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/courtview-api ./cmd/courtview-api

FROM alpine:3.20
RUN addgroup -S app && adduser -S app -G app \
    && apk add --no-cache ca-certificates tzdata wget

WORKDIR /app
COPY --from=build /out/courtview-api /usr/local/bin/courtview-api

ENV SERVICE_ADDR=:8088
ENV COURTVIEW_BASE_URL=https://records.courts.alaska.gov/eaccess/home.page.2
ENV DB_MAX_SIZE_MB=100
ENV DB_LOG_MAX_SIZE_MB=10
ENV DB_PURGE_TARGET_MB=80

EXPOSE 8088
USER app

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8088/healthz >/dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/courtview-api"]
