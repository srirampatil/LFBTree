/*
 * Entry.hpp
 *
 *  Created on: Apr 13, 2014
 *      Author: sriram
 */

#ifndef ENTRY_HPP_
#define ENTRY_HPP_

#include <atomic>

class Entry {
private:
	std::atomic<long> keyData;
	std::atomic<Entry *> next;

	bool nextFreezeBit;bool deleteBit;

public:
	Entry();
	virtual ~Entry();
};

#endif /* ENTRY_HPP_ */
