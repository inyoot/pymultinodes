#!/bin/bash
python multinode.py dispatcher secret &
sleep 2
SERVER_PID=$!
time python demo/primes.py
kill -n 1 $SERVER_PID
