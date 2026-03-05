
FROM golang:1.26-alpine AS builder

WORKDIR /app


COPY ../../go.mod ../../go.sum ./



RUN go mod download


COPY . .


RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-w -s" -o server ./cmd/server


FROM alpine:latest


RUN apk --no-cache add ca-certificates && \
    addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup


RUN mkdir -p /home/appuser/.config/kvst && \
     chown -R appuser:appgroup /home/appuser/.config

USER appuser

WORKDIR /home/appuser/


COPY --from=builder /app/server .

EXPOSE 9999

ENTRYPOINT ["./server"]