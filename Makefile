EXES=client node testClients/tc1 testClients/tc2 testClients/tc3 testClients/tc4

all:$(EXES)

client: client.go kvservice/*.go common/*.go
	go build -o client client.go
node: kvnode.go kvnode/*.go common/*.go
	go build -o node kvnode.go
testClients/tc1: common/*.go kvservice/*.go testClients/*.go
	go build -o testClients/tc1 testClients/tc1.go
testClients/tc2: common/*.go kvservice/*.go testClients/*.go
	go build -o testClients/tc2 testClients/tc2.go
testClients/tc3: common/*.go kvservice/*.go testClients/*.go
	go build -o testClients/tc3 testClients/tc3.go
testClients/tc4: common/*.go kvservice/*.go testClients/*.go
	go build -o testClients/tc4 testClients/tc4.go


.PHONY: clean
clean:
	-@rm node client >/dev/null
