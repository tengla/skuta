FROM golang:1.26 AS builder
ARG VERSION
ARG GIT_COMMIT
ARG BUILD_TIME
ENV GOPATH=/go
ENV PATH=$GOPATH:$PATH
ENV ADDR=0.0.0.0
ADD . $GOPATH/github.com/tengla/skuta
WORKDIR $GOPATH/github.com/tengla/skuta
RUN go mod vendor
RUN CGO_ENABLED=0 go build -a -mod=vendor -installsuffix cgo -ldflags="-w -s -X main.version=$VERSION" -o skuta-run .
FROM registry.access.redhat.com/ubi8/ubi-minimal:latest
ENV GOPATH=/go
ENV PATH=$GOPATH/bin:$PATH
RUN mkdir /skuta
COPY --from=builder $GOPATH/github.com/tengla/skuta/skuta-run ./skuta
COPY entrypoint.sh /
RUN chown -R 1001:root  /skuta && chmod g+rwX  /skuta
EXPOSE 50000
EXPOSE 9090
EXPOSE 8080
#RUN adduser -D skuta
WORKDIR skuta
USER 1001
CMD ["./skuta-run","server"]

