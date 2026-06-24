FROM golang:1.22-alpine AS build
WORKDIR /src
COPY services/proxy/go.mod ./
COPY services/proxy/main.go ./
RUN CGO_ENABLED=0 go build -o /ztap-proxy .

FROM alpine:3.20
RUN apk add --no-cache wget
COPY --from=build /ztap-proxy /usr/local/bin/ztap-proxy
EXPOSE 5432 8000
ENTRYPOINT ["/usr/local/bin/ztap-proxy"]
