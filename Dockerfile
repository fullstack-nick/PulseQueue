FROM golang:1.26.4 AS build

WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/pulsequeue ./cmd/pulsequeue

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /app
COPY --from=build /out/pulsequeue /usr/local/bin/pulsequeue
COPY migrations ./migrations

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/pulsequeue"]
