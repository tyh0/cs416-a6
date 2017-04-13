#!/bin/bash
killall node
make

# test using local loopback with 2 nodes and node death
HOSTPORTA=9000
CLIENTPORTA=9001
HOSTPORTB=9002
CLIENTPORTB=9003

# printf "%s\n%s" "localhost:9000" "localhost:9002" > nodefile.txt
# # printf "%s\n%s" "localhost:$CLIENTPORTA" "localhost:$CLIENTPORTB" > clientfile.txt

# printf "%s\n%s" "localhost:9001" "localhost:9003" > clientfile.txt

./node nodefile.txt 1 ":$HOSTPORTA" ":$CLIENTPORTA" &
sleep 1
./node nodefile.txt 1 ":$HOSTPORTB" ":$CLIENTPORTB" &
sleep 10

./testClients/tc3 clientfile.txt && ./testClients/tc4 clientfile.txt
