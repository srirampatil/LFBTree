/*
 * Chunk.cpp
 *
 *  Created on: Apr 13, 2014
 *      Author: sriram
 */

#include "LChunk.hpp"
#include <string.h>
#include <bitset>

using namespace std;

template <class TData>
const char LChunk<TData>::LEntry::DEFAULT_KEY = '|';

void hello() {
	cout << "Hello " << this_thread::get_id() << endl;
}

template<typename T>
void show_binrep(const T& a)
{
    const char* beg = reinterpret_cast<const char*>(&a);
    const char* end = beg + sizeof(a);
    while(beg != end)
        std::cout << std::bitset<CHAR_BIT>(*beg++) << ' ';
    std::cout << '\n';
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

	LChunk<char *> *chunk = new LChunk<char *>(4, 10);
}
