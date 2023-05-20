FROM golang:1.20-alpine3.18 AS builder

WORKDIR /build

COPY ./ /build

RUN go build -o dist/tmdb-scraper ./cmd/scraper

FROM alpine:3.18

COPY --from=builder /build/dist/tmdb-scraper /usr/local/bin/tmdb-scraper

RUN mkdir /posters && chmod 0777 /posters

WORKDIR /

CMD [ "/usr/local/bin/tmdb-scraper" ]
