/*
 * Chunk.cpp
 *
 *  Created on: Apr 13, 2014
 *      Author: sriram
 */

#include "Chunk.hpp"
#include <string.h>

using namespace std;

template <class TData>
const char Chunk<TData>::Entry::DEFAULT_KEY = '|';

void hello() {
	cout << "Hello " << this_thread::get_id() << endl;
}

int main() {
	std::thread t(hello);
	int a;
	cout << &a << endl;
	t.join();

	cout << "Bing!" << endl;
	cout << sizeof(long) << endl;

	char *c = (char *) "Sriram";
	char *c2 = (char *)"Patil";
	atomic<char *> ac;
	ac.store((char *) "Sriram");

	char *c3 = (char *)"Sriram";

	ac.compare_exchange_strong(c3, c2);
	cout << strlen(ac) << endl;

	cout << sizeof(char) << endl;

	Chunk<char *> *chunk = new Chunk<char *>(4, 10);
}
