/*
 * Chunk.hpp
 *
 *  Created on: Apr 13, 2014
 *      Author: sriram
 */

#ifndef CHUNK_HPP_
#define CHUNK_HPP_

#include <iostream>
#include <thread>
#include <map>
#include <atomic>
#include <vector>

#define HALF_LONG sizeof(long) / 2

#define KEY_MASK 0xffffffff00000000
#define DATA_MASK 0x00000000ffffffff

#define SET_FREEZE_BIT_MASK 2
#define SET_DELETE_BIT_MASK 1
#define UNSET_FREEZE_BIT_MASK 0xfffffffffffffffd
#define UNSET_DELETE_BIT_MASK 0xfffffffffffffffe

#define SET_FREEZE_STATE_MASK 7
#define UNSET_FREEZE_STATE_MASK 0xfffffffffffffff8

typedef int RecovType;

enum TriggerType {
	NONE, INSERT, DELETE, ENSLAVE
};

enum ReturnCode {
	SUCCESS_THIS, SUCCESS_OTHER, EXISTED
};

enum FreezeState {
	NO_FREEZE, INTERNAL_FREEZE
};

template<class TData>
class Chunk {
public:

	class Entry {
    private:
        std::atomic<long> keyData;
		
		std::atomic<Entry *> next;		// LSBs are deleteBit and freezeBit
		// bool freezeBit:1;
		// bool deleteBit:1;

	public:
		static const int DEFAULT_KEY;

		Entry() : next(NULL) {
			keyData = (long) DEFAULT_KEY;
			keyData = keyData << HALF_LONG;

			// Preserve upper 4 bytes and mask lower 4 bytes
			keyData &= KEY_MASK;
		}

		virtual ~Entry() {
		}

		static long combine(int key, TData data) {
			long combinedKeyData = (long) key;
			combinedKeyData <<= HALF_LONG;
			combinedKeyData |= (long) data;
			return combinedKeyData;
		}

		/**
		 * Checks if the frozen bit (second LSB) is set in a given pointer p and
		 * returns true or false accordingly.
		 */
		bool isFrozen() {
			return ((long)next & SET_FREEZE_BIT_MASK);
		}

		/**
		 * Returns the value of a pointer p with the frozen bit set to one; it doesn’t
		 * matter if in initial p this bit was set or not.
		 */
		void markFrozen() {
			next |= SET_FREEZE_BIT_MASK;
		}

		/**
		 * Returns the value of a pointer p with the frozen bit reset to zero; it
		 * doesn’t matter if in initial p this bit was set or not.
		 */
		void clearFrozen() {
			next &= UNSET_FREEZE_BIT_MASK;
		}
	};

	std::atomic<int> counter;
	std::vector<Entry *> entriesArray;
	std::atomic<Chunk *> newChunk;

	std::atomic<Chunk *> mergeBuddy;		// last 3 LSBs are freezeState
	// int freezeState:3;

	std::atomic<Chunk *> nextChunk;

	int MAX, MIN;

	static __thread Entry *cur;
	static __thread Entry **previous;
	static __thread Entry *next;

	static __thread Entry **hp0;
	static __thread Entry **hp1;

	Entry* AllocateEntry(Chunk* chunk, int key, TData data) {
		long keyData = Entry::combine(key, data);
		// Combine into the structure of a keyData word
		long expecEnt = Entry::combine(Entry::DEFAULT_KEY, 0);

		// Traverse entries in chunk
		for (unsigned i = 0; i < entriesArray.size(); i++) {
			Entry *e = entriesArray[i];

			if (e->keyData == expecEnt) {
				// Atomically try to allocate
				if (atomic_compare_exchange_strong(&(e->keyData), &expecEnt,
						keyData))
					return e;
			}
		}

		return NULL;	// No free entry was found
	}

public:

	Chunk(int max, int min) : counter(0), newChunk(NULL), mergeBuddy(NULL), nextChunk(NULL), 
			MAX(max), MIN(min) {
		for (int i = 0; i < MAX; i++)
			entriesArray.push_back(new Entry());

		setFreezeState(NO_FREEZE);
	}

	virtual ~Chunk() {
	}

	bool compareAndSetFreezeState(FreezeState oldState, FreezeState newState) {
		Chunk *oldMergeBuddy = getMergeBuddy();
		oldMergeBuddy  = (long) oldMergeBuddy | oldState;

		Chunk *newMergeBuddy = (long) oldMergeBuddy | newState;
		
		return compareAndSetMergeBuddy(oldMergeBuddy, newMergeBuddy);
	}

	int	getFreezeState() {
		return (long) mergeBuddy & SET_FREEZE_STATE_MASK;
	}

	Chunk *getMergeBuddy() {
		return mergeBuddy & UNSET_FREEZE_STATE_MASK;
	}

	bool compareAndSetMergeBuddy(Chunk *oldMergeBuddy, Chunk *newMergeBuddy) {
		return mergeBuddy.compare_exchange_strong(oldMergeBuddy, newMergeBuddy);
	}

	bool Search(int key, TData *data) {
		Chunk* chunk = FindChunk(key);
		bool result = SearchInChunk(chunk, key, data);
		// hp5 = hp4 = hp3 = hp2 = null;
		return result;
	}

	bool Insert(int key, TData data) {
		Chunk* chunk = FindChunk(key);
		bool result = InsertToChunk(chunk, key, data);
		// hp5 = hp4 = hp3 = hp2 = null;
		return result;
	}

	bool Delete(int key, TData data) {
		Chunk* chunk = FindChunk(key);
		bool result = DeleteInChunk(chunk, key);
		// hp5 = hp4 = hp3 = hp2 = null;
		return result;
	}

	void IncCount(Chunk* chunk) {
		while (true) {
			int oldCounter = chunk->counter;

			if (chunk->counter.compare_exchange_strong(oldCounter,
					oldCounter + 1))
				return;
		}
	}

	bool DecCount(Chunk* chunk) {
		while (true) {
			int oldCounter = chunk->counter;

			if (oldCounter == MIN)
				return false;
			// comparison with minimal, MIN-1 illegal

			if (chunk->counter.compare_exchange_strong(oldCounter,
					oldCounter - 1))
				return true;
		}
	}

	bool InsertToChunk(Chunk* chunk, int key, TData data) {
		// Find an available entry
		Entry *current = AllocateEntry(chunk, key, data);

		bool result;
		while (current == NULL) {
			// No available entry. Freeze and try again
			chunk = Freeze(chunk, key, data, INSERT, &result);
			if (chunk == NULL)
				return result;
			// Freeze completed the insertion.
			current = AllocateEntry(chunk, key, data);
			// Otherwise, retry allocation
		}
		ReturnCode code = InsertEntry(chunk, current, key);

		switch (code) {
		case SUCCESS_THIS:
			IncCount(chunk);
			result = true;
			break; // Increments the entries’ counter in the chunk

		case SUCCESS_OTHER:
			// Entry was inserted by other thread due to help in freeze

			result = true;
			break;

		case EXISTED:
			// This key exists in the list. Reclaim entry

			if (ClearEntry(chunk, current))
				// Attempt to clear the entry

				result = false;

			else
				// Failure to clear the entry implies that a freeze thread

				result = true;
			// eventually inserts the entry

			break;
		} // end of switch
		*hp0 = *hp1 = NULL;
		return result;
		// Clear all hazard pointers and return
	}

	ReturnCode InsertEntry(Chunk* chunk, Entry* entry, int key) {
		while (true) {

			// Find insert location and pointers to previous and current entries
			Entry *savedNext = entry->next;

			if (Find(chunk, key)) {
				// This key existed in the list, cur is global initiated by Find
				if (entry == cur)
					return SUCCESS_OTHER;
				else
					return EXISTED;
			}

			// Find method sets cur to a entry which should be the next to "entry"
			// and prev holds the entry which will point to "entry"
			if (savedNext->isFrozen())
				cur->markFrozen();
			// cur will replace savedNext

			if (cur->isFrozen())
				entry->markFrozen();
			// entry will replace cur

			// Attempt linking into the list

			if (!entry->next.compare_exchange_strong(savedNext, cur))
				continue;
			// Attempt setting next field

			// TODO: Check if correct
			if (!(*previous)->next.compare_exchange_strong(cur, entry))
				continue; // Attempt linking, prev is global initiated by Find

			return SUCCESS_THIS;
			// both CASes were successful
		}
	}

	Chunk* Freeze(Chunk* chunk, int key, TData data, TriggerType trigger,
				bool* result) {
			
    }

	void MarkChunkFrozen(Chunk* chunk);
	void StabilizeChunk(Chunk* chunk);
	RecovType FreezeDecision(Chunk* chunk);

	Chunk* FreezeRecovery(Chunk* oldChunk, int key, int input, RecovType recov,
			Chunk* mergeChunk, TriggerType trigger, bool* result);

	Chunk* HelpInFreezeRecovery(Chunk* newChunk1, Chunk* newChunk2, int key,
			int separatKey, int input, TriggerType trigger);

	Chunk* FindMergeSlave(Chunk* master);

	bool SearchInChunk(Chunk* chunk, int key, int *data);bool Find(Chunk* chunk,
			int key);bool DeleteInChunk(Chunk* chunk, int key);

	void RetireEntry(Entry* entry);
	void HandleReclamationBuffer();bool ClearEntry(Chunk* chunk, Entry* entry);

	void ListUpdate(RecovType recov, int key, Chunk* chunk);

	bool HelpSwap(Chunk* expected);

	Chunk* FindChunk(int key);

	Chunk* listFindPrevious(Chunk* chunk);
};

#endif /* CHUNK_HPP_ */
