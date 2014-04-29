/*
 * Entry.cpp
 *
 *  Created on: Apr 29, 2014
 *      Author: spatil
 */

#include <cstdlib>
#include <cstdio>
#include "Entry.hpp"

Entry::Entry() :
		nextEntry(0) {
	setKey(Utils::DEFAULT_KEY);
}

Entry::~Entry() {
}

long Entry::getKey() {
	uint128 tempKeyData = keyData >> 64;
	uint128 mask = 0x0000000000000000ffffffffffffffff;
	uint128 k = tempKeyData & mask;
	return (long) k;
}

long Entry::getData() {
	uint128 mask = 0x0000000000000000fffffffffffffffe;
	uint128 k = keyData & mask;
	return (long) k;
}

long Entry::getDataWithFreezeBit() {
	uint128 mask = 0x0000000000000000ffffffffffffffff;
	uint128 k = keyData & mask;
	return (long) k;
}

bool Entry::setKey(long newKey) {
	uint128 oldKeyData = keyData;
	uint128 newKeyData = Utils::combine(newKey, getDataWithFreezeBit());

	return (oldKeyData
			== Utils::InterlockedCompareExchange128(&keyData, oldKeyData,
					newKeyData));
}

bool Entry::setData(long newData) {
	uint128 oldKeyData = keyData;
	uint128 newKeyData = Utils::combine(getKey(), newData);

	return (oldKeyData
			== Utils::InterlockedCompareExchange128(&keyData, oldKeyData,
					newKeyData));
}

bool Entry::isFrozen(uint128 value) {
	return ((value & 1) == 1);
}

uint128 Entry::markFrozen(uint128 value) {
	return value | 1;
}

uint128 Entry::clearFrozen(uint128 value) {
	return value & 0xfffffffffffffffe;
}

bool Entry::isDeleted(Entry *e) {
	return (((long) e & 2) == 2);
}

Entry * Entry::markDeleted(Entry *e) {
	return (Entry *) ((long) e | 2);
}

Entry * Entry::clearDeleted(Entry *e) {
	return (Entry *) ((long) e & 0xfffffffffffffffd);
}
