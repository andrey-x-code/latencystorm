FROM golang:1.26.10-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY go-common/ go-common/
COPY demo-target/ demo-target/
RUN go mod download && CGO_ENABLED=0 go build -mod=readonly -trimpath -o /out/app ./demo-target
FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/app /app
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/app"]
