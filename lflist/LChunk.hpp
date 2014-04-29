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

#include "Utils.hpp"

using namespace std;

//#define HALF_LONG sizeof(long) / 2

#define KEY_MASK 0xffffffff00000000
#define DATA_MASK 0x00000000ffffffff

#define SET_FREEZE_BIT_MASK 1
#define SET_DELETE_BIT_MASK 2
#define UNSET_FREEZE_BIT_MASK 0xfffffffffffffffe
#define UNSET_DELETE_BIT_MASK 0xfffffffffffffffd

#define SET_FREEZE_STATE_MASK 7
#define UNSET_FREEZE_STATE_MASK 0xfffffffffffffff8
#define SET_SWAPPED_BIT_MASK 1
#define UNSET_SWAPPED_BIT_MASK 0xfffffffffffffffe

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
class LChunk {
public:

	class LEntry {
	private:
		__uint128_t keyData;	// key, data and freezeBit

		std::atomic<LEntry *> next;		// LSBs are deleteBit and freezeBit
		// bool freezeBit:1;
		// bool deleteBit:1;

	public:
		static const char DEFAULT_KEY;

		LEntry() :
				keyData(0), next(NULL) {
			/* CHANGE for (unsigned i = 0; i < sizeof(long double); i++)
			 keyData[i] = 0;

			 keyData[7] = DEFAULT_KEY; */

			// Preserve upper 4 bytes and mask lower 4 bytes
			// keyData &= KEY_MASK;
		}

		virtual ~LEntry() {
		}

		/* static char *combineKeyData(long key, TData data) {
		 char *combinedKeyData = new char[sizeof(long double)];

		 long mask = 0x00000000000000ff;
		 for (unsigned i = 7, j = 15; i >= 0 && j >= 8; i--, j--) {
		 char byte = key & mask;
		 combinedKeyData[i] = byte;

		 byte = data & mask;
		 combinedKeyData[j] = byte;

		 mask = mask << sizeof(char);
		 }

		 return combinedKeyData;
		 } */

		/**
		 * Checks if the frozen bit (second LSB) is set in a given pointer p and
		 * returns true or false accordingly.
		 */
		static bool isFrozen(LEntry *word) {
			return (word & SET_FREEZE_BIT_MASK);
		}

		/**
		 * Returns the value of a pointer p with the frozen bit set to one; it doesn’t
		 * matter if in initial p this bit was set or not.
		 */
		static LEntry *markFrozen(LEntry *word) {
			return word | SET_FREEZE_BIT_MASK;
		}

		/**
		 * Returns the value of a pointer p with the frozen bit reset to zero; it
		 * doesn’t matter if in initial p this bit was set or not.
		 */
		static LEntry *clearFrozen(LEntry *word) {
			return word & UNSET_FREEZE_BIT_MASK;
		}

		/*
		 * Checks if deleted bit (LSB) is set in given pointer p.
		 */
		static bool isDeleted(LEntry *word) {
			return (word & SET_DELETE_BIT_MASK);
		}

		/*
		 * Returns the value of a pointer p with the deleted bit set to one; it doesn’t 
		 * matter if in initial p this bit was set or not.
		 */
		static LEntry *markDeleted(LEntry *entr) {
			return (entr | SET_DELETE_BIT_MASK);
		}

		/*
		 * Returns the value of a pointer p with the deleted bit reset to zero; it
		 * does not matter if in initial p this bit was set or not.
		 */
		static LEntry *clearDeleted(LEntry *entr) {
			return (entr & UNSET_DELETE_BIT_MASK);
		}

		long key() {
			__uint128_t mask = 0xffffffffffffffff0000000000000000;
			__uint128_t k = keyData & mask;
			k = k >> 64;
			return (long) k;
		}
	};

	std::atomic<int> counter;
	LEntry *head;
	std::vector<LEntry *> entriesArray;
	std::atomic<LChunk *> newChunk;

	std::atomic<LChunk *> mergeBuddy;		// last 3 LSBs are freezeState
	// int freezeState:3;

	std::atomic<LChunk *> nextChunk;

	int MAX, MIN;

	static __thread LEntry *cur;
	static __thread LEntry **previous;
	static __thread LEntry *next;

	static __thread LEntry **hp0;
	static __thread LEntry **hp1;

	LEntry* AllocateEntry(LChunk* chunk, long key, TData data) {
		__uint128_t newKeyData = Utils::combine(key, data);
		// Combine into the structure of a keyData word
		__uint128_t expecEnt = Utils::combine(LEntry::DEFAULT_KEY, 0);

		// Traverse entries in chunk
		for (unsigned i = 0; i < entriesArray.size(); i++) {
			LEntry *e = entriesArray[i];

			if (e->keyData == expecEnt) {
				// Atomically try to allocate
				//if (e->keyData.compare_exchange_strong(expecEnt, newKeyData))
				__uint128_t oldKeyData = e->keyData;
				if (oldKeyData
						== Utils::InterlockedCompareExchange128(&(e->keyData),
								expecEnt, newKeyData))
					return e;
			}
		}

		return NULL;	// No free entry was found
	}

public:

	LChunk(int max, int min) :
			counter(0), newChunk(NULL), mergeBuddy(NULL), nextChunk(NULL), MAX(
					max), MIN(min) {
		/* for (int i = 0; i < MAX; i++)
		 entriesArray.push_back(new Entry()); */

		head = new LEntry();
		entriesArray.push_back(head);
		compareAndSetFreezeState(NO_FREEZE, NO_FREEZE);
	}

	virtual ~LChunk() {
	}

	/*
	 * Checks if swapped bit (LSB) is set in given pointer to a chunk c.
	 */
	static bool isSwapped(LChunk *chunk) {
		return (chunk & SET_SWAPPED_BIT_MASK);
	}

	/*
	 * Returns the value of a pointer c with the swapped bit set to one; it
	 * does not matter if in initial c this bit was set or not.
	 */
	static LChunk *markSwapped(LChunk *chunk) {
		return (chunk | SET_SWAPPED_BIT_MASK);
	}

	/*
	 * Returns the value of a pointer c with the swapped bit set to zero; it
	 * does not matter if in initial c this bit was set or not.
	 */
	static LChunk *clearSwapped(LChunk *chunk) {
		return (chunk & UNSET_SWAPPED_BIT_MASK);
	}

	static LChunk *combineChunkState(LChunk *chunk, FreezeState state) {
		return chunk | state;
	}

	static LChunk *Allocate() {
		return new LChunk();
	}

	bool compareAndSetFreezeState(FreezeState oldState, FreezeState newState) {
		LChunk *oldMergeBuddy = (LChunk *) (((long) getMergeBuddy())
				& UNSET_FREEZE_STATE_MASK);
		oldMergeBuddy = (LChunk *) (((long) oldMergeBuddy) | oldState);

		LChunk *newMergeBuddy = (LChunk *) (((long) oldMergeBuddy) | newState);

		return compareAndSetMergeBuddyAndFreezeState(oldMergeBuddy,
				newMergeBuddy);
	}

	int getFreezeState() {
		return (long) mergeBuddy & SET_FREEZE_STATE_MASK;
	}

	LChunk *getMergeBuddy() {
		return mergeBuddy;	// & UNSET_FREEZE_STATE_MASK;
	}

	bool compareAndSetMergeBuddy(LChunk *oldMergeBuddy, LChunk *newMergeBuddy) {
		FreezeState currentState = getFreezeState();
		return compareAndSetMergeBuddyAndFreezeState(
				oldMergeBuddy | currentState, newMergeBuddy | currentState);
	}

	bool compareAndSetMergeBuddyAndFreezeState(LChunk *oldMergeBuddy,
			LChunk *newMergeBuddy) {
		return mergeBuddy.compare_exchange_strong(oldMergeBuddy, newMergeBuddy);
	}

	/*
	 * Goes over all reachable entries in Chunk c, counts them, and returns the
	 * number of entries. Chunk c is assumed to be frozen and thus cannot be
	 * modified.
	 */
	int getEntrNum(LChunk *chunk) {
		LEntry *entr = chunk->head->next;
		int count = 0;

		while (LEntry::clearFrozen(entr) != NULL) {
			count++;
			entr = entr->next;
		}

		return count;
	}

	/*
	 * Goes over all reachable entries in the old chunk linked list and copies
	 * them to the new chunk linked list. It is assumed no other thread is
	 * modifying the new chunk, and that the old chunk is frozen, so it cannot
	 * be modified as well.
	 */
	static void copyToOneChunk(LChunk *oldChunk, LChunk *chunk) {
		LEntry *entr = LEntry::clearFrozen(oldChunk->head->next);
		chunk->head->next = entr;

		LEntry *prevEntr = chunk->head;
		while (entr != NULL) {
			chunk->entriesArray.push_back(entr);
			prevEntr->next = entr;
			prevEntr = entr;
			entr = LEntry::clearFrozen(entr->next);
		}
	}

	/*
	 * Goes over all reachable entries on the old1 and old2 chunks linked lists
	 * (which are sequential and have enough entries to fill one chunk’s linked
	 * list) and copies them to the new chunk linked list. It is assumed that no
	 * other thread modifies the new chunk and that the old chunks are frozen
	 * and thus don’t change.
	 */
	static void mergeToOneChunk(LChunk *old1, LChunk *old2, LChunk *new1) {
		LEntry *entr = LEntry::clearFrozen(old1->head->next);
		new1->head->next = entr;

		LEntry *prevEntr = new1->head;
		while (entr != NULL) {
			new1->entriesArray.push_back(entr);
			prevEntr->next = entr;
			prevEntr = entr;
			entr = LEntry::clearFrozen(entr->next);
		}

		entr = LEntry::clearFrozen(old2->head->next);
		while (entr != NULL) {
			new1->entriesArray.push_back(entr);
			prevEntr->next = entr;
			prevEntr = entr;
			entr = LEntry::clearFrozen(entr->next);
		}
	}

	/*
	 * Goes over all reachable entries on the old chunk linked list, finds the
	 * median key (which is returned) and copies the bellow-median-value keys
	 * to the new1 chunk and the above-median-value keys to the new2 chunk.
	 * In addition it sets the new1 chunk’s pointer nextChunk to point at
	 * the new2 chunk. It is assumed that no other thread is modifying the
	 * new1 and new2 chunks, and that the old chunk is frozen and cannot be
	 * modified.
	 */
	static long splitIntoTwoChunks(LChunk *old, LChunk *new1, LChunk *new2) {
		LEntry *slowPtr = LEntry::clearFrozen(old->head->next);
		LEntry *fastPtr = slowPtr;
		LEntry *fastPtrNext = LEntry::clearFrozen(fastPtr->next);

		while (fastPtr != NULL && fastPtrNext != NULL) {
			fastPtr = LEntry::clearFrozen(fastPtrNext->next);
			fastPtrNext = LEntry::clearFrozen(fastPtr->next);
			slowPtr = LEntry::clearFrozen(slowPtr->next);
		}

		LEntry *median = slowPtr;

		LEntry *entr = LEntry::clearFrozen(old->head->next);
		LEntry *prevEntr = new1->head;
		while (entr != median) {
			new1->entriesArray.push_back(entr);
			prevEntr->next = entr;
			prevEntr = entr;
			entr = LEntry::clearFrozen(entr->next);
		}

		entr = LEntry::clearFrozen(median->next);
		while (entr != NULL) {
			new1->entriesArray.push_back(entr);
			prevEntr->next = entr;
			prevEntr = entr;
			entr = LEntry::clearFrozen(entr->next);
		}

		return median->key();
	}

	/*
	 * Goes over all reachable entries in the old1 and old2 chunks linked lists
	 * (which are sequential), finds the median key (which is returned) and
	 * copies the bellow-median-value keys to the new1 chunk linked list and
	 * the above-median-value keys to the new2 chunk linked list. In addition
	 * it sets the new1 chunk’s pointer nextChunk to point to the new2 chunk.
	 * It is assumed that no other thread modifies the new1 and new2 chunks,
	 * and that the old chunks are frozen and thus cannot be modified as well.
	 */
	static long mergeToTwoChunks(LChunk *old1, LChunk *old2, LChunk *new1,
			LChunk *new2) {
		LEntry *newListHead = LEntry::clearFrozen(old1->head->next);
		LEntry *entr = newListHead, listNode = newListHead;

		while (entr != NULL) {
			entr = LEntry::clearFrozen(entr->next);
			listNode->next = entr;
		}

		entr = LEntry::clearFrozen(old2->head->next);
		listNode->next = entr;
		while (entr != NULL) {
			entr = LEntry::clearFrozen(entr->next);
			listNode->next = entr;
		}

		LEntry *slowPtr = newListHead;
		LEntry *fastPtr = slowPtr;
		LEntry *fastPtrNext = fastPtr->next;

		while (fastPtr != NULL && fastPtrNext != NULL) {
			fastPtr = fastPtrNext->next;
			fastPtrNext = fastPtr->next;
			slowPtr = slowPtr->next;
		}

		LEntry *median = slowPtr;
		listNode = newListHead;
		while (listNode != median) {
			new1->entriesArray.push_back(listNode);
			listNode = listNode->next;
		}

		listNode = median->next;
		while (listNode != NULL) {
			new2->entriesArray.push_back(listNode);
			listNode = listNode->next;
		}

		return median->key();
	}

	bool Search(long key, TData *data) {
		LChunk* chunk = FindChunk(key);
		bool result = SearchInChunk(chunk, key, data);
		// hp5 = hp4 = hp3 = hp2 = null;
		return result;
	}

	bool Insert(long key, TData data) {
		LChunk* chunk = FindChunk(key);
		bool result = InsertToChunk(chunk, key, data);
		// hp5 = hp4 = hp3 = hp2 = null;
		return result;
	}

	bool Delete(long key, TData data) {
		LChunk* chunk = FindChunk(key);
		bool result = DeleteInChunk(chunk, key);
		// hp5 = hp4 = hp3 = hp2 = null;
		return result;
	}

	void IncCount(LChunk* chunk) {
		while (true) {
			int oldCounter = chunk->counter;

			if (chunk->counter.compare_exchange_strong(oldCounter,
					oldCounter + 1))
				return;
		}
	}

	bool DecCount(LChunk* chunk) {
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

	bool InsertToChunk(LChunk* chunk, long key, TData data) {
		// Find an available entry
		LEntry *current = AllocateEntry(chunk, key, data);

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

	ReturnCode InsertEntry(LChunk* chunk, LEntry* entry, long key) {
		while (true) {

			// Find insert location and pointers to previous and current entries
			LEntry *savedNext = entry->next;

			if (Find(chunk, key)) {
				// This key existed in the list, cur is global initiated by Find
				if (entry == cur)
					return SUCCESS_OTHER;
				else
					return EXISTED;
			}

			// Find method sets cur to a entry which should be the next to "entry"
			// and prev holds the entry which will point to "entry"
			if (LEntry::isFrozen(savedNext))
				cur = LEntry::markFrozen(cur);
			// cur will replace savedNext

			if (LEntry::isFrozen(cur))
				entry = LEntry::markFrozen(entry);
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

	LChunk* Freeze(LChunk* chunk, long key, TData data, TriggerType trigger,
	bool* result) {

		chunk->compareAndSetFreezeState(NO_FREEZE, INTERNAL_FREEZE);
		// At this point, the freeze state is either internal freeze or external freeze
		MarkChunkFrozen(chunk);
		StabilizeChunk(chunk);

		if (chunk->getFreezeState() == EXTERNAL_FREEZE) {
			// This chunk was in external freeze before Line 1 executed. Find the master chunk.
			LChunk *master = chunk->getMergeBuddy();
			// Fix the buddy’s mergeBuddy pointer.
			LChunk *masterOldBuddy = LChunk::combineChunkState(NULL,
					INTERNAL_FREEZE);
			LChunk *masterNewBuddy = LChunk::combineChunkState(chunk,
					INTERNAL_FREEZE);
			master->compareAndSetMergeBuddyAndFreezeState(masterOldBuddy,
					masterNewBuddy);
			return FreezeRecovery(chunk->getMergeBuddy(), key, data, MERGE,
					chunk, trigger, result);
		}

		RecovType decision = FreezeDecision(chunk);
		// The freeze state is internal freeze

		LChunk *chunkMergePartner = NULL;
		if (decision == MERGE)
			chunkMergePartner = FindMergeSlave(chunk);

		return FreezeRecovery(chunk, key, data, decision, chunkMergePartner,
				trigger, result);
	}

	void MarkChunkFrozen(LChunk* chunk) {
		for (unsigned i = 0; i < entriesArray.size(); i++) {
			LEntry *e = entriesArray[i];
			long savedWord = (long) e->next;

			while (!LEntry::isFrozen((long) savedWord)) {
				// Loop till the next pointer is frozen

				e->next.compare_exchange_strong(savedWord,
						LEntry::markFrozen((long) savedWord));

				savedWord = e->next;
				// Reread from shared memory

			}

			savedWord = e->keyData;

			while (!LEntry::isFrozen((long) savedWord)) {
				// Loop till the keyData word is frozen

				Utils::InterlockedCompareExchange128( &(e->keyData), savedWord,
						LEntry::markFrozen((long) savedWord));

				savedWord = e->keyData;
				// Reread from shared memory

			}
		} // end of foreach

		return;
	}

	void StabilizeChunk(LChunk* chunk) {
		int maxKey = LONG_MAX;
		Find(chunk, maxKey);
		// Implicitly remove deleted entries
		for (unsigned i = 0; i < entriesArray.size(); i++) {
			LEntry *e = entriesArray[i];
			long key = e->key();
			LEntry *eNext = e->next;

			if ((key != LEntry::DEFAULT_KEY) && (!isDeleted(eNext))) {
				// This entry is allocated and not deleted

				if (!Find(chunk, key))
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
	RecovType FreezeDecision(LChunk* chunk) {
		LEntry* e = chunk->head->next;
		int cnt = 0;

		// Going over the chunk’s list
		while (LEntry::clearFrozen(e) != NULL) {
			cnt++;
			e = e->next;
		}

		if (cnt == MIN)
			return MERGE;
		if (cnt == MAX)
			return SPLIT;

		return COPY;
	}

	LChunk* FreezeRecovery(LChunk* oldChunk, long key, TData input,
			RecovType recovType, LChunk* mergeChunk, TriggerType trigger,
			bool* result) {
		LChunk *retChunk = NULL, *newChunk2 = NULL, *newChunk1 =
				LChunk::Allocate();
		// Allocate a new chunk
		newChunk1->nextChunk = NULL;

		long separatKey = 0;

		switch (recovType) {
		case COPY:
			copyToOneChunk(oldChunk, newChunk1);
			break;

		case MERGE:

			if ((getEntrNum(oldChunk) + getEntrNum(mergeChunk)) >= MAX) {

				// The two neighboring old chunks will be merged into two new chunks

				newChunk2 = LChunk::Allocate();
				// Allocate a second new chunk

				newChunk1->nextChunk = newChunk2;
				// Connect two chunks together

				newChunk2->nextChunk = NULL;

				separatKey = mergeToTwoChunks(oldChunk, mergeChunk, newChunk1,
						newChunk2);

			} else
				mergeToOneChunk(oldChunk, mergeChunk, newChunk1); // Merge to single chunk

			break;

		case SPLIT:

			newChunk2 = LChunk::Allocate();
			// Allocate a second new chunk

			newChunk1->nextChunk = newChunk2;
			// Connect two chunks together

			newChunk2->nextChunk = NULL;

			separatKey = splitIntoTwoChunks(oldChunk, newChunk1, newChunk2);

			break;
		} // end of switch

		// Perform the operation with which the freeze was initiated
		HelpInFreezeRecovery(newChunk1, newChunk2, key, separatKey, input,
				trigger);
		// Try to create a link to the first new chunk in the old chunk.
		if (!CAS(&(oldChunk->newChunk), NULL, newChunk1)) {
			RetireChunk(newChunk1);
			if (newChunk2)
				RetireChunk(newChunk2);
			// Determine in which of the new chunks the key is located.
			if (key < separatKey)
				retChunk = oldChunk->newChunk;
			else
				retChunk = FindChunk(key);
		} else {
			retChunk = NULL;
		}
		ListUpdate(recovType, key, oldChunk);
		// User defined function
		return retChunk;
	}

	LChunk* HelpInFreezeRecovery(LChunk* newChunk1, LChunk* newChunk2, long key,
			int separatKey, int input, TriggerType trigger);

	LChunk* FindMergeSlave(LChunk* master);

	bool SearchInChunk(LChunk* chunk, long key, TData *data);bool Find(
			LChunk* chunk, long key);bool DeleteInChunk(LChunk* chunk, long key);

	void RetireEntry(LEntry* entry);
	void HandleReclamationBuffer();bool ClearEntry(LChunk* chunk, LEntry* entry);

	void ListUpdate(RecovType recov, long key, LChunk* chunk);

	bool HelpSwap(LChunk* expected);

	LChunk* FindChunk(long key);

	LChunk* listFindPrevious(LChunk* chunk);
};

#endif /* CHUNK_HPP_ */
