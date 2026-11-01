FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /gateway .

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /gateway /gateway
COPY config.docker.yaml /config.yaml
USER 65534
ENTRYPOINT ["/gateway", "--config", "/config.yaml"]
