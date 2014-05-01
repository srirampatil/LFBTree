//============================================================================
// Name        : LFBTree.cpp
// Author      : Jigar Kaneriya
// Version     :
// Copyright   : Your copyright notice
// Description :
//============================================================================

#include <iostream>
using namespace std;

#include "lfbtree.hpp"


int main() {
	BTree b(10,20);
	b.FindJoinSlave(new Node(10,10));
	cout << "Btree" <<(long)NULL << endl; // prints
	return 0;
}
