FROM golang
WORKDIR /go/src/github.com/goavro/wednesday
COPY . /go/src/github.com/goavro/wednesday
RUN go get -u github.com/Masterminds/glide
CMD go test -v -tags=integration $(glide novendor)
