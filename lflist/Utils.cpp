/*
 * Utils.cpp
 *
 *  Created on: Apr 28, 2014
 *      Author: spatil
 */

#include "Utils.hpp"

const char Utils::DEFAULT_KEY = '|';

Utils::Utils() {
	// TODO Auto-generated constructor stub

}

Utils::~Utils() {
	// TODO Auto-generated destructor stub
}

uint128 Utils::combine(long msb, long lsb) {
	uint128 combinedBits = (uint128) msb;
	combinedBits = combinedBits << 64;
	combinedBits = combinedBits | (uint128) lsb;
	return combinedBits;
}

