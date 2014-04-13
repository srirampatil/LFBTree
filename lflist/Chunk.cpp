/*
 * Chunk.cpp
 *
 *  Created on: Apr 13, 2014
 *      Author: sriram
 */

#include "Chunk.hpp"

using namespace std;

template <class TData>
const int Chunk<TData>::Entry::DEFAULT_KEY = (int) '|';

void hello() {
	cout << "Hello " << this_thread::get_id() << endl;
}

int main() {
	std::thread t(hello);
	int a;
	cout << &a << endl;
	t.join();
}
