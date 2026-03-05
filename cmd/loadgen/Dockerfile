# Build stage
FROM golang:1.26-alpine AS builder

WORKDIR /app


COPY ../../go.mod ../../go.sum ./



RUN go mod download


COPY . .


RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-w -s" -o loadgen ./cmd/loadgen


FROM alpine:latest


RUN apk --no-cache add ca-certificates && \
    addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup

# switch to non-root user
USER appuser

WORKDIR /home/appuser/


COPY --from=builder /app/loadgen .

ENTRYPOINT ["./loadgen"]