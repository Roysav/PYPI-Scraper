FROM golang

COPY go.mod main.go app/

WORKDIR /app

RUN go build main.go

CMD ["./main"]
