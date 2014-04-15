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
#include <limits.h>
#include <cstdlib>

//#define HALF_LONG sizeof(long) / 2

#define KEY_MASK 0xffffffff00000000
#define DATA_MASK 0x00000000ffffffff

#define SET_FREEZE_BIT_MASK 1
#define SET_DELETE_BIT_MASK 2
#define UNSET_FREEZE_BIT_MASK 0xfffffffffffffffe
#define UNSET_DELETE_BIT_MASK 0xfffffffffffffffd

#define SET_FREEZE_STATE_MASK 7
#define UNSET_FREEZE_STATE_MASK 0xfffffffffffffff8

enum RecovType {
	SPLIT, MERGE, COPY
};

enum TriggerType {
	NONE, INSERT, DELETE, ENSLAVE
};

enum ReturnCode {
	SUCCESS_THIS, SUCCESS_OTHER, EXISTED
};

enum FreezeState {
	NO_FREEZE = 0, INTERNAL_FREEZE, EXTERNAL_FREEZE
};

template<class TData>
class Chunk {
public:

	class Entry {
    private:
        std::atomic<char *> keyData;	// key, data and freezeBit
		
		std::atomic<Entry *> next;		// LSBs are deleteBit and freezeBit
		// bool freezeBit:1;
		// bool deleteBit:1;

	public:
		static const char DEFAULT_KEY;

		Entry() : next(NULL) {
			for(unsigned i = 0; i < sizeof(long double); i++)
				keyData[i] = 0;
			
			keyData[7] = DEFAULT_KEY;

			// Preserve upper 4 bytes and mask lower 4 bytes
			keyData &= KEY_MASK;
		}

		virtual ~Entry() {
		}

		static char *combineKeyData(long key, TData data) {
			char *combinedKeyData = new char[sizeof(long double)];
			
			long mask = 0x00000000000000ff;
			for(unsigned i = 7, j = 15; i >= 0 && j >= 8; i--, j--) {
				char byte = key & mask;
				combinedKeyData[i] = byte;

				byte = data & mask;
				combinedKeyData[j] = byte;

				mask = mask << sizeof(char);
			}

			return combinedKeyData;
		}

		/**
		 * Checks if the frozen bit (second LSB) is set in a given pointer p and
		 * returns true or false accordingly.
		 */
		static bool isFrozen(long word) {
			return (word & SET_FREEZE_BIT_MASK);
		}

		/**
		 * Returns the value of a pointer p with the frozen bit set to one; it doesn’t
		 * matter if in initial p this bit was set or not.
		 */
		static long markFrozen(long word) {
			return word | SET_FREEZE_BIT_MASK;
		}

		/**
		 * Returns the value of a pointer p with the frozen bit reset to zero; it
		 * doesn’t matter if in initial p this bit was set or not.
		 */
		static long clearFrozen(long word) {
			return word & UNSET_FREEZE_BIT_MASK;
		}

		static bool isDeleted(long word) {
			return (word & SET_DELETE_BIT_MASK);
		}

		long key() {
			char keyStr[sizeof(long) + 1];
			long retKey = 0;
			for(unsigned i = 0; i < sizeof(long); i++)
				keyStr[i]= keyData[i];
			keyStr[sizeof(long)] = '\0';

			retKey = atol(keyStr);
			delete[] keyStr;
			return retKey;
		}
	};

	std::atomic<int> counter;
	Entry *head;
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
		char *newKeyData = Entry::combineKeyData(key, data);
		// Combine into the structure of a keyData word
		char *expecEnt = Entry::combineKeyData(Entry::DEFAULT_KEY, 0);

		// Traverse entries in chunk
		for (unsigned i = 0; i < entriesArray.size(); i++) {
			Entry *e = entriesArray[i];

			if (strcmp(e->keyData, expecEnt) == 0) {
				// Atomically try to allocate
				if (e->keyData.compare_exchange_strong(expecEnt, newKeyData))
					return e;
			}
		}

		return NULL;	// No free entry was found
	}

public:

	Chunk(int max, int min) : counter(0), newChunk(NULL), mergeBuddy(NULL), nextChunk(NULL), 
			MAX(max), MIN(min) {
		/* for (int i = 0; i < MAX; i++)
			entriesArray.push_back(new Entry()); */

		head = new Entry();
		entriesArray.push_back(head);
		compareAndSetFreezeState(0, NO_FREEZE);
	}

	virtual ~Chunk() {
	}

	static Chunk *combineChunkState(Chunk *chunk, FreezeState state) {
		return chunk | state;
	}

	bool compareAndSetFreezeState(FreezeState oldState, FreezeState newState) {
		Chunk *oldMergeBuddy = getMergeBuddy() & UNSET_FREEZE_STATE_MASK;
		oldMergeBuddy  = (long) oldMergeBuddy | oldState;

		Chunk *newMergeBuddy = (long) oldMergeBuddy | newState;
		
		return compareAndSetMergeBuddyAndFreezeState(oldMergeBuddy, newMergeBuddy);
	}

	int	getFreezeState() {
		return (long) mergeBuddy & SET_FREEZE_STATE_MASK;
	}

	Chunk *getMergeBuddy() {
		return mergeBuddy;// & UNSET_FREEZE_STATE_MASK;
	}

	bool compareAndSetMergeBuddy(Chunk *oldMergeBuddy, Chunk *newMergeBuddy) {
		FreezeState currentState = getFreezeState();
		return compareAndSetMergeBuddyAndFreezeState(oldMergeBuddy | currentState, newMergeBuddy | currentState); 
	}

	bool compareAndSetMergeBuddyAndFreezeState(Chunk *oldMergeBuddy, Chunk *newMergeBuddy) {
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
			if (Entry::isFrozen(savedNext))
				cur = Entry::markFrozen(cur);
			// cur will replace savedNext

			if (Entry::isFrozen(cur))
				entry = Entry::markFrozen(entry);
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

		chunk->compareAndSetFreezeState(NO_FREEZE, INTERNAL_FREEZE);
		// At this point, the freeze state is either internal freeze or external freeze
		MarkChunkFrozen(chunk);
		StabilizeChunk(chunk);

		if ( chunk->getFreezeState() == EXTERNAL_FREEZE ) {
			// This chunk was in external freeze before Line 1 executed. Find the master chunk.
			Chunk *master = chunk->getMergeBuddy();
			// Fix the buddy’s mergeBuddy pointer.
			Chunk *masterOldBuddy = Chunk::combineChunkState(NULL, INTERNAL_FREEZE);
			Chunk *masterNewBuddy = Chunk::combineChunkState(chunk, INTERNAL_FREEZE);
			master->compareAndSetMergeBuddyAndFreezeState(masterOldBuddy, masterNewBuddy);
			return FreezeRecovery(chunk->getMergeBuddy(), key, data, MERGE, chunk, trigger, result);
		}

		RecovType decision = FreezeDecision(chunk);
		// The freeze state is internal freeze
		
		Chunk *chunkMergePartner = NULL;
		if ( decision == MERGE ) 
			chunkMergePartner = FindMergeSlave(chunk);
		
		return FreezeRecovery(chunk, key, data, decision, chunkMergePartner, trigger, result);
    }

	void MarkChunkFrozen(Chunk* chunk) {
		for(unsigned i = 0; i < entriesArray.size(); i++) {
			Entry *e = entriesArray[i];
			long savedWord = (long) e->next;
			
			while ( !Entry::isFrozen((long) savedWord) ) {
				// Loop till the next pointer is frozen
				
				e->next.compare_exchange_strong(savedWord, Entry::markFrozen((long) savedWord));
				
				savedWord = e->next;
				// Reread from shared memory
				
			}
			
			savedWord = e->keyData;
			
			while ( !Entry::isFrozen((long) savedWord) ) {
				// Loop till the keyData word is frozen
				
				e->keyDatacompare_exchange_strong(savedWord, Entry::markFrozen((long) savedWord));
				
				savedWord = e->keyData;
				// Reread from shared memory
				
			}
		} // end of foreach
		
		return;
	}

	void StabilizeChunk(Chunk* chunk) {
		int maxKey = INT_MAX;
		Find(chunk, maxKey);
		// Implicitly remove deleted entries
		for(unsigned i = 0; i < entriesArray.size(); i++) {
			Entry *e = entriesArray[i];
			int key = e->key();
			Entry *eNext = e->next;
		 
			if ((key != Entry::DEFAULT_KEY) && (!isDeleted(eNext)) ) {
		 	// This entry is allocated and not deleted
		 
		 		if ( !Find(chunk, key) )
					InsertEntry(chunk, e, key);
			}
		 	// This key is not yet in the list
		 } // end of foreach

		 return;
	}

	/**
	 * Checks the count of entries to decide operation to perform after freezing.
	 * Multiple threads can delete or add entries o this method is important.
	 */
	RecovType FreezeDecision(Chunk* chunk) {
		Entry* e = chunk->head->next;
		int cnt = 0;
		
		// Going over the chunk’s list
		while ( Entry::clearFrozen(e) != NULL ) {
			cnt++;
			e = e->next;
		}

		if ( cnt == MIN)
			return MERGE; 
		if ( cnt == MAX)
			return SPLIT;
			
		return COPY;
	}

	Chunk* FreezeRecovery(Chunk* oldChunk, int key, int input, RecovType recov,
			Chunk* mergeChunk, TriggerType trigger, bool* result);

	Chunk* HelpInFreezeRecovery(Chunk* newChunk1, Chunk* newChunk2, int key,
			int separatKey, int input, TriggerType trigger);

	Chunk* FindMergeSlave(Chunk* master);

	bool SearchInChunk(Chunk* chunk, int key, int *data);
	bool Find(Chunk* chunk,int key);
	bool DeleteInChunk(Chunk* chunk, int key);

	void RetireEntry(Entry* entry);
	void HandleReclamationBuffer();
	bool ClearEntry(Chunk* chunk, Entry* entry);

	void ListUpdate(RecovType recov, int key, Chunk* chunk);

	bool HelpSwap(Chunk* expected);

	Chunk* FindChunk(int key);

	Chunk* listFindPrevious(Chunk* chunk);
};

#endif /* CHUNK_HPP_ */
