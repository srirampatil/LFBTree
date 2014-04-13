/*
 * Chunk.cpp
 *
 *  Created on: Apr 13, 2014
 *      Author: sriram
 */

#include "Chunk.hpp"

#include <cstdbool>
#include <cstdlib>

using namespace std;

Chunk::Chunk() {
	// TODO Auto-generated constructor stub

}

Chunk::~Chunk() {
	// TODO Auto-generated destructor stub
}

bool Chunk::Search(int key, int *data) {
	return true;
}

bool Chunk::Insert(int key, int data) {
	return true;
}

bool Chunk::Delete(int key, int data) {
	return true;
}

bool Chunk::InsertToChunk(Chunk* chunk, int key, int data) {
	return true;
}

Chunk* Chunk::Freeze(Chunk* chunk, int key, int data, triggerType trigger,
bool* result) {
	return NULL;
}

returnCode Chunk::InsertEntry(Chunk* chunk, Entry* entry, int key) {
	return 0;
}

void Chunk::MarkChunkFrozen(Chunk* chunk) {

}

void Chunk::StabilizeChunk(Chunk* chunk) {

}

recovType Chunk::FreezeDecision(Chunk* chunk) {
	return 0;
}

Chunk* Chunk::FreezeRecovery(Chunk* oldChunk, int key, int input,
		recovType recov, Chunk* mergeChunk, triggerType trigger, bool* result) {
	return NULL;
}

Chunk* Chunk::HelpInFreezeRecovery(Chunk* newChunk1, Chunk* newChunk2, int key,
		int separatKey, int input, triggerType trigger) {
	return NULL;
}

Chunk* Chunk::FindMergeSlave(Chunk* master) {
	return NULL;
}

bool Chunk::SearchInChunk(Chunk* chunk, int key, int *data) {
	return true;
}

bool Chunk::Find(Chunk* chunk, int key) {
	return true;
}

bool Chunk::DeleteInChunk(Chunk* chunk, int key) {
	return true;
}

void Chunk::RetireEntry(Entry* entry) {

}

void Chunk::HandleReclamationBuffer() {

}

bool Chunk::ClearEntry(Chunk* chunk, Entry* entry) {
	return true;
}

void Chunk::IncCount(Chunk* chunk) {

}

bool Chunk::DecCount(Chunk* chunk) {
	return true;
}

void Chunk::ListUpdate(recovType recov, int key, Chunk* chunk) {

}

bool Chunk::HelpSwap(Chunk* expected) {
	return true;
}

Chunk* Chunk::FindChunk(int key) {
	return NULL;
}

Chunk* Chunk::listFindPrevious(Chunk* chunk) {
	return NULL;
}

