FROM golang:1.26-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o erebor-signals ./cmd/erebor-signals

FROM alpine:3
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=builder /build/erebor-signals .
EXPOSE 8080
ENTRYPOINT ["/app/erebor-signals"]
