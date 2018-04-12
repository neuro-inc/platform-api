FROM golang:1.10.1-stretch AS build

WORKDIR /go/src/github.com/neuromation/platform-api

COPY api api
COPY log log
COPY vendor vendor
COPY main.go Gopkg.* ./

RUN go build

FROM golang:1.10.1-stretch

COPY --from=build /go/src/github.com/neuromation/platform-api/platform-api ./

EXPOSE 8080

CMD ["./platform-api"]
