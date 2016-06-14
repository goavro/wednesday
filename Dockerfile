FROM golang
WORKDIR /go/src/github.com/yanzay/wednesday
COPY . /go/src/github.com/yanzay/wednesday
RUN go get -u github.com/Masterminds/glide
CMD go test -v -tags=integration $(glide novendor)
