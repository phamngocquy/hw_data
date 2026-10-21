FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -ldflags "-X main.version=${VERSION}" -o /out/ingestor .

FROM gcr.io/distroless/static-debian12
WORKDIR /app
COPY --from=build /out/ingestor /usr/local/bin/ingestor
ENV LOG_FILE=/app/logs/etl.log \
    HTTP_ADDR=:8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/ingestor"]
