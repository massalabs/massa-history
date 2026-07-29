FROM alpine:latest

ENV BUF_VERSION=1.47.2
ENV PROTOC_GEN_DOC_VERSION=1.5.1

RUN apk update && apk upgrade
RUN apk --no-cache --update add bash protoc curl protobuf-dev && rm -rf /var/cache/apk/*

WORKDIR /tmp

# install buf
RUN curl -sSL \
    "https://github.com/bufbuild/buf/releases/download/v$BUF_VERSION/buf-$(uname -s)-$(uname -m)" \
    -o "/usr/bin/buf" && \
    chmod +x "/usr/bin/buf" && rm -rf ./*

# install protoc-gen-doc
RUN curl -sSL \
    "https://github.com/pseudomuto/protoc-gen-doc/releases/download/v$PROTOC_GEN_DOC_VERSION/protoc-gen-doc_${PROTOC_GEN_DOC_VERSION}_linux_amd64.tar.gz" --output protoc-gen-doc.tar.gz && \
    tar -xf ./protoc-gen-doc.tar.gz && cp ./protoc-gen-doc /usr/bin/ && rm -rf ./*


WORKDIR /opt
CMD ["bash", "build.sh"]
