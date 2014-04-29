/*
 * Entry.hpp
 *
 *  Created on: Apr 29, 2014
 *      Author: spatil
 */

#ifndef ENTRY_HPP_
#define ENTRY_HPP_

#include <cstdlib>
#include <atomic>

#include "Utils.hpp"

class Entry {
public:
	volatile uint128 keyData;					// key, data and free bit
	std::atomic<long> nextEntry;	// next entry, delete bit and freeze bit

	Entry();

	virtual ~Entry();

	long getKey();

	long getData();

	long getDataWithFreezeBit();

	bool setKey(long newKey);

	bool setData(long newData);

	static bool isFrozen(uint128 value);

	static uint128 markFrozen(uint128 value);

	static uint128 clearFrozen(uint128 value);

	static bool isDeleted(Entry *e);

	static Entry *markDeleted(Entry *e);

	static Entry *clearDeleted(Entry *e);
};

#endif /* ENTRY_HPP_ */
