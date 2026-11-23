FROM golang:1.26.10-bookworm AS build
WORKDIR /src
COPY contracts/go/ /contracts/go/
COPY src/go.mod src/go.sum ./
COPY src/go-common/ go-common/
COPY src/demo-target/ demo-target/
RUN go mod download && CGO_ENABLED=0 go build -mod=readonly -trimpath -o /out/app ./demo-target
FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/app /app
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/app"]
