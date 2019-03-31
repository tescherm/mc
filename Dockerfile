FROM golang:1.12-alpine3.9 AS builder

# install build dependencies
RUN apk add --no-cache make
COPY . /go/src/github.com/tescherm/mc
WORKDIR /go/src/github.com/tescherm/mc
RUN make clean binary

FROM alpine:3.9

RUN apk add --no-cache ca-certificates

COPY --from=builder /go/src/github.com/tescherm/mc/build/bin/mc /

ENTRYPOINT ["/mc"]
