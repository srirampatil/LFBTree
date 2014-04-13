/*
 * Chunk.hpp
 *
 *  Created on: Apr 13, 2014
 *      Author: sriram
 */

#ifndef CHUNK_HPP_
#define CHUNK_HPP_

#include "Entry.hpp"
#include <atomic>
#include <vector>

typedef int recovType;
typedef int triggerType;
typedef int returnCode;

class Chunk {
private:
	std::atomic<int> counter;
	std::atomic<int> freezeState;

	std::atomic<Chunk *> mergeBuddy;
	std::atomic<Chunk *> nextChunk;
	std::atomic<Chunk *> newChunk;

	std::vector<Entry> entriesArray;

	Entry* AllocateEntry(Chunk* chunk, int key, int data);

public:
	Chunk();
	virtual ~Chunk();

	bool Search(int key, int *data);bool Insert(int key, int data);bool Delete(
			int key, int data);

	bool InsertToChunk(Chunk* chunk, int key, int data);
	Chunk* Freeze(Chunk* chunk, int key, int data, triggerType trigger,
			bool* result);
	returnCode InsertEntry(Chunk* chunk, Entry* entry, int key);

	void MarkChunkFrozen(Chunk* chunk);
	void StabilizeChunk(Chunk* chunk);
	recovType FreezeDecision(Chunk* chunk);

	Chunk* FreezeRecovery(Chunk* oldChunk, int key, int input, recovType recov,
			Chunk* mergeChunk, triggerType trigger, bool* result);

	Chunk* HelpInFreezeRecovery(Chunk* newChunk1, Chunk* newChunk2, int key,
			int separatKey, int input, triggerType trigger);

	Chunk* FindMergeSlave(Chunk* master);

	bool SearchInChunk(Chunk* chunk, int key, int *data);bool Find(Chunk* chunk,
			int key);bool DeleteInChunk(Chunk* chunk, int key);

	void RetireEntry(Entry* entry);
	void HandleReclamationBuffer();bool ClearEntry(Chunk* chunk, Entry* entry);

	void IncCount(Chunk* chunk);bool DecCount(Chunk* chunk);

	void ListUpdate(recovType recov, int key, Chunk* chunk);

	bool HelpSwap(Chunk* expected);

	Chunk* FindChunk(int key);

	Chunk* listFindPrevious(Chunk* chunk);
};

#endif /* CHUNK_HPP_ */
