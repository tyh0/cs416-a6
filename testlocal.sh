#!/bin/bash

# test using local loopback with one node

make
HOSTPORT=9000
CLIENTPORT=9001

echo "localhost:$HOSTPORT" > localnodes.txt
echo "localhost:$CLIENTPORT" > clientnodes.txt

./node localnodes.txt 1 ":$HOSTPORT" ":$CLIENTPORT" &

sleep 3

./client clientnodes.txt &
wait %%
