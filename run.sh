#!/bin/bash

g++ -O0 -g3 -Wall -c -fmessage-length=0 -std=c++0x -MMD -MP -MF"lflist/Chunk.d" -MT"lflist/Chunk.d" -o "lflist/Chunk.o" "./lflist/Chunk.cpp"
g++ -o "LFBTree" ./lflist/Chunk.o -lpthread
./LFBTree
