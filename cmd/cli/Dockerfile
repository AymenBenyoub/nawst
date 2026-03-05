
FROM golang:1.26-alpine AS builder

WORKDIR /app


COPY ../../go.mod ../../go.sum ./


RUN go mod download


COPY . .


RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-w -s" -o cli ./cmd/cli


FROM alpine:latest


RUN apk --no-cache add ca-certificates && \
    addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup


USER appuser

WORKDIR /home/appuser/


COPY --from=builder /app/cli .


ENTRYPOINT ["./cli"]