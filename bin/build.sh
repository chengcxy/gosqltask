cd ../cmd/
echo $PWD
echo "go build -o gosqltask main.go"
go build -o gosqltask main.go
echo "$GOPATH/bin"
mv gosqltask $GOPATH/bin/

