#!/bin/bash

#rm -rf LFBTree

g++ -g3 -Wall -c -std=c++0x -MMD -MF"lflist/Chunk.d" -MT"lflist/Chunk.d" -o "lflist/Chunk.o" "./lflist/Chunk.cpp"
g++ -o "LFBTree" ./lflist/Chunk.o -lpthread
./LFBTree
