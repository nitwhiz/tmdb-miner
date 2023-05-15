FROM golang:1.20-alpine3.18 AS builder

WORKDIR /build

COPY ./ /build

RUN go build -o dist/tmdb-miner ./cmd/scraper

FROM alpine:3.18

WORKDIR /opt/tmdb-miner

COPY --from=builder /build/dist/tmdb-miner /opt/tmdb-miner/tmdb-miner

RUN mkdir /opt/tmdb-miner/posters

CMD [ "/opt/tmdb-miner/tmdb-miner" ]
