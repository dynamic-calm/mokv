FROM golang:1.25 AS build
WORKDIR /build/src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags=-s -o bin/mokv .
FROM scratch
COPY --from=build /build/src/bin/mokv /usr/bin/mokv
ENTRYPOINT [ "/usr/bin/mokv" ]

