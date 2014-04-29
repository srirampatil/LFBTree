/*
 * Chunk.hpp
 *
 *  Created on: Apr 28, 2014
 *      Author: spatil
 */

#ifndef CHUNK_HPP_
#define CHUNK_HPP_

#include <thread>
#include <atomic>
#include <cstdbool>
#include <cstdlib>
#include <iostream>
#include <limits.h>

#include "Utils.hpp"
#include "Entry.hpp"

#define SET_FREEZE_STATE_MASK 7
#define UNSET_FREEZE_STATE_MASK 0xfffffffffffffff8
#define SET_SWAPPED_BIT_MASK 1
#define UNSET_SWAPPED_BIT_MASK 0xfffffffffffffffe

//namespace lflist {

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
private:

	int MIN, MAX;
	std::atomic<long> counter;		// number of entries currently allocated
	Entry *head;
	Entry *entriesArray;
	std::atomic<Chunk *> newChunk;
	std::atomic<long> mergeBuddy;			// merge ptr and freeze state
	std::atomic<Chunk *> nextChunk;

public:

	static __thread Entry **prev;
	static __thread Entry *cur;
	static __thread Entry *next;

	static __thread Entry **hp0;
	static __thread Entry **hp1;
	static __thread Entry **hp2;
	static __thread Entry **hp3;
	static __thread Entry **hp4;
	static __thread Entry **hp5;

	Chunk(int min, int max) :
			MIN(min), MAX(max), counter(0), newChunk(NULL), mergeBuddy(0), nextChunk(
			NULL) {
		if (MAX <= (2 * MIN + 1)) {
			std::cerr << "MAX > (2 * MIN + 1): Condition violated!"
					<< std::endl;
			exit(1);
		}

		entriesArray = new Entry[MAX + 1];		// +1 for dummy header
		for (int i = 0; i <= MAX; i++)
			entriesArray[i] = new Entry();

		head = entriesArray[0];
		mergeBuddy = (mergeBuddy | NO_FREEZE);
	}

	virtual ~Chunk() {
	}

	/*
	 * Checks if swapped bit (LSB) is set in given pointer to a chunk c.
	 */
	static bool isSwapped(Chunk *chunk) {
		return (chunk & SET_SWAPPED_BIT_MASK);
	}

	/*
	 * Returns the value of a pointer c with the swapped bit set to one; it
	 * does not matter if in initial c this bit was set or not.
	 */
	static Chunk *markSwapped(Chunk *chunk) {
		return (chunk | SET_SWAPPED_BIT_MASK);
	}

	/*
	 * Returns the value of a pointer c with the swapped bit set to zero; it
	 * does not matter if in initial c this bit was set or not.
	 */
	static Chunk *clearSwapped(Chunk *chunk) {
		return (chunk & UNSET_SWAPPED_BIT_MASK);
	}

	static Chunk *combineChunkState(Chunk *chunk, FreezeState state) {
		return chunk | state;
	}

	static Chunk *allocate() {
		return new Chunk(Utils::MIN_KEYS, Utils::MAX_KEYS);
	}

	bool compareAndSetFreezeState(FreezeState oldState, FreezeState newState) {
		long oldMergeBuddy = ((long) getMergeBuddy()) & UNSET_FREEZE_STATE_MASK;
		oldMergeBuddy = ((long) oldMergeBuddy) | oldState;

		long newMergeBuddy = ((long) oldMergeBuddy) | newState;

		return compareAndSetMergeBuddyAndFreezeState(oldMergeBuddy,
				newMergeBuddy);
	}

	int getFreezeState() {
		return (long) mergeBuddy & SET_FREEZE_STATE_MASK;
	}

	long getMergeBuddy() {
		return mergeBuddy;	// & UNSET_FREEZE_STATE_MASK;
	}

	bool compareAndSetMergeBuddy(long oldMergeBuddy, long newMergeBuddy) {
		FreezeState currentState = getFreezeState();
		return compareAndSetMergeBuddyAndFreezeState(
				oldMergeBuddy | currentState, newMergeBuddy | currentState);
	}

	bool compareAndSetMergeBuddyAndFreezeState(long oldMergeBuddy,
			long newMergeBuddy) {
		return mergeBuddy.compare_exchange_strong(oldMergeBuddy, newMergeBuddy);
	}

	/*
	 * Goes over all reachable entries in Chunk c, counts them, and returns the
	 * number of entries. Chunk c is assumed to be frozen and thus cannot be
	 * modified.
	 */
	int getEntrNum(Chunk *chunk) {
		Entry *entr = chunk->head->nextEntry.load();
		int count = 0;

		while (Entry::clearFrozen((long) entr) != 0) {
			count++;
			entr = entr->nextEntry.load();
		}

		return count;
	}

	/*
	 * Goes over all reachable entries in the old chunk linked list and copies
	 * them to the new chunk linked list. It is assumed no other thread is
	 * modifying the new chunk, and that the old chunk is frozen, so it cannot
	 * be modified as well.
	 */
	static void copyToOneChunk(Chunk *oldChunk, Chunk *chunk) {
		Entry *entr = (Entry *) Entry::clearFrozen(
				(long) oldChunk->head->nextEntry.load());
		chunk->head->nextEntry.store(entr);

		Entry *prevEntr = chunk->head;
		int index = 1;

		while (entr != NULL) {
			chunk->entriesArray[index] = entr;
			prevEntr->nextEntry.store((long) entr);
			prevEntr = entr;
			entr = (Entry *) Entry::clearFrozen(entr->nextEntry.load());
			index++;
		}
	}

	/*
	 * Goes over all reachable entries on the old1 and old2 chunks linked lists
	 * (which are sequential and have enough entries to fill one chunk’s linked
	 * list) and copies them to the new chunk linked list. It is assumed that no
	 * other thread modifies the new chunk and that the old chunks are frozen
	 * and thus don’t change.
	 */
	static void mergeToOneChunk(Chunk *old1, Chunk *old2, Chunk *new1) {
		Entry *entr = (Entry *) Entry::clearFrozen(
				(long) old1->head->nextENtry.load());
		new1->head->nextEntry.store(entr);

		Entry *prevEntr = new1->head;
		int index = 1;
		while (entr != NULL) {
			new1->entriesArray[index] = entr;
			prevEntr->nextEntry.store((long) entr);
			prevEntr = entr;
			entr = (Entry *) Entry::clearFrozen(entr->nextEntry.load());
			index++;
		}

		entr = Entry::clearFrozen(old2->head->next);
		while (entr != NULL) {
			new1->entriesArray[index] = entr;
			prevEntr->nextEntry.store((long) entr);
			prevEntr = entr;
			entr = (Entry *) Entry::clearFrozen((long) entr->nextEntry.load());
			index++;
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
	static long splitIntoTwoChunks(Chunk *old, Chunk *new1, Chunk *new2) {
		Entry *slowPtr = (Entry *) Entry::clearFrozen(
				(long) old->head->nextEntry.load());
		Entry *fastPtr = slowPtr;
		Entry *fastPtrNext = (Entry *) Entry::clearFrozen(
				(long) fastPtr->nextEntry.load());

		while (fastPtr != NULL && fastPtrNext != NULL) {
			fastPtr = (Entry *) Entry::clearFrozen(
					(long) fastPtrNext->nextEntry.load());
			fastPtrNext = (Entry *) Entry::clearFrozen(
					(long) fastPtr->nextEntry.load());
			slowPtr = (Entry *) Entry::clearFrozen(
					(long) slowPtr->nextEntry.load());
		}

		Entry *median = slowPtr;

		Entry *entr = (Entry *) Entry::clearFrozen(
				(long) old->head->nextEntry.load());
		Entry *prevEntr = new1->head;
		int index = 1;
		while (entr != median) {
			new1->entriesArray[index] = entr;
			prevEntr->nextEntry.store((long) entr);
			prevEntr = entr;
			entr = (Entry *) Entry::clearFrozen((long) entr->nextEntry.load());
			index++;
		}

		entr = (Entry *) Entry::clearFrozen((long) median->nextEntry.load());
		index = 1;
		prevEntr = new2->head;
		while (entr != NULL) {
			new2->entriesArray[index] = entr;
			prevEntr->nextEntry.store((long) entr);
			prevEntr = entr;
			entr = (Entry *) Entry::clearFrozen((long) entr->nextEntry.load());
			index++;
		}

		return median->getKey();
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
	static long mergeToTwoChunks(Chunk *old1, Chunk *old2, Chunk *new1,
			Chunk *new2) {
		Entry *newListHead = (Entry *) Entry::clearFrozen(
				(long) old1->head->nextEntry.load());
		Entry *entr = newListHead, *listNode = newListHead;

		while (entr != NULL) {
			entr = (Entry *) Entry::clearFrozen((long) entr->nextEntry.load());
			listNode->nextEntry.store((long) entr);
		}

		entr = (Entry *) Entry::clearFrozen(
				(long) old2->head->nextEntry.load());
		listNode->nextEntry.store((long) entr);
		while (entr != NULL) {
			entr = (Entry *) Entry::clearFrozen((long) entr->nextEntry.load());
			listNode->nextEntry.store((long) entr);
		}

		Entry *slowPtr = newListHead;
		Entry *fastPtr = slowPtr;
		Entry *fastPtrNext = fastPtr->nextEntry.load();

		while (fastPtr != NULL && fastPtrNext != NULL) {
			fastPtr = fastPtrNext->nextEntry.load();
			fastPtrNext = fastPtr->nextEntry.load();
			slowPtr = slowPtr->nextEntry.load();
		}

		Entry *median = slowPtr;
		listNode = newListHead;
		int index = 1;
		while (listNode != median) {
			new1->entriesArray[index] = listNode;
			listNode = listNode->nextEntry.load();
			index++;
		}

		listNode = median->nextEntry.load();
		while (listNode != NULL) {
			new2->entriesArray[index] = listNode;
			listNode = listNode->nextEntry.load();
			index++;
		}

		return median->getKey();
	}

	bool search(long key, TData data) {
		Chunk *chunk = findChunk(key);
		bool result = searchInChunk(chunk, key, data);
		hp5 = hp4 = hp3 = hp2 = NULL;
		return result;
	}

	bool insert(long key, TData data) {
		Chunk *chunk = findChunk(key);
		bool result = insertToChunk(chunk, key, data);
		hp5 = hp4 = hp3 = hp2 = NULL;
		return result;
	}

	bool deleteChunk(long key, TData data) {
		Chunk *chunk = findChunk(key);
		bool result = deleteInChunk(chunk, key, data);
		hp5 = hp4 = hp3 = hp2 = NULL;
		return result;
	}

	bool searchInChunk(Chunk *chunk, long key, TData data);
	Chunk *findChunk(long key);

	bool insertToChunk(Chunk *chunk, long key, TData data) {
		bool result;
		Entry *current = allocateEntry(chunk, key, data); // Find an available entry

		while (current == NULL) {	// No available entry freeze and try again
			chunk = freeze(chunk, key, data, INSERT, &result);
			if (chunk == NULL)
				return result;		// freeze completed insertion
			current = allocateEntry(chunk, key, data);		// retry allocation
		}

		ReturnCode retCode = insertEntry(chunk, current, key);
		switch (retCode) {
		case SUCCESS_THIS:
			incCount(chunk);
			result = true;
			break;

		case SUCCESS_OTHER:
			result = true;
			break;

		case EXISTED:
			if (clearEntry(chunk, current))
				return false;
			return true;
			break;
		}

		*hp0 = *hp1 = NULL;
		return result;
	}

	bool deleteInChunk(Chunk *chunk, long key, TData data);

	Entry *allocateEntry(Chunk * chunk, long key, TData data) {
		uint128 newKeyData = Utils::combine(key, data);

		// default value
		uint128 expectedKeyData = Utils::combine(Utils::DEFAULT_KEY, 0);

		// look for some empty entry
		for (int i = 0; i < MAX; i++) {
			if (chunk->entriesArray[i].keyData == expectedKeyData) {
				uint128 oldKeyData = chunk->entriesArray[i].keyData;
				if (oldKeyData
						== Utils::InterlockedCompareExchange128(
								&(chunk->entriesArray[i].keyData), oldKeyData,
								newKeyData))
					return chunk->entriesArray[i];
			}
		}

		return NULL;
	}

	Chunk *freeze(Chunk *chunk, long key, TData data, TriggerType trigger,
	bool *result) {
		chunk->compareAndSetFreezeState(NO_FREEZE, INTERNAL_FREEZE);

		markChunkFrozen(chunk);
		stabilizeChunk(chunk);

		if (chunk->getFreezeState() == EXTERNAL_FREEZE) {
			Chunk *master = chunk->mergeBuddy;
			Chunk *masterOldMergeBuddy = combineChunkState(NULL,
					INTERNAL_FREEZE);
			Chunk *masterNewBuddy = combineChunkState(chunk, INTERNAL_FREEZE);

			master->compareAndSetMergeBuddyAndFreezeState(masterOldMergeBuddy,
					masterNewBuddy);

			return freezeRecovery(chunk->mergeBuddy, key, data, MERGE, chunk,
					trigger, result);
		}

		RecovType decision = freezeDecision(chunk);
		Chunk *mergePartner = NULL;
		if (decision == MERGE)
			mergePartner = findMergeSlave(chunk);
		return freezeRecovery(chunk, key, data, decision, mergePartner, trigger,
				result);
	}

	ReturnCode insertEntry(Chunk * chunk, Entry *entry, long key) {
		while (true) {
			Entry *savedNext = entry->nextEntry.load();
			// Find insert location and pointers to current and previous entries
			if (find(chunk, key)) {
				if (entry == cur)
					return SUCCESS_OTHER;
				return EXISTED;
			}

			// If neighbourhood is frozen keep it frozen
			if (Entry::isFrozen((uint128) savedNext))
				Entry::markFrozen((uint128) cur);

			if (Entry::isFrozen((uint128) cur))
				Entry::markFrozen((uint128) entry);

			if (!std::atomic_compare_exchange_strong(&(entry->nextEntry),
					(long *) &savedNext, (long) cur))
				continue;

			// TODO: Changed this
			if (!std::atomic_compare_exchange_strong(&((*prev)->nextEntry),
					(long *) &cur, (long) entry))
				continue;

			return SUCCESS_THIS;
		}
	}

	bool clearEntry(Chunk *chunk, Entry *entry);

	void incCount(Chunk *chunk) {
		long cnt;
		while(true) {
			cnt = chunk->counter.load();

			if(atomic_compare_exchange_strong(&(chunk->counter), cnt, cnt + 1))
				return;
		}
	}

	bool decCount(Chunk *chunk) {
		int cnt;
		while(true) {
			cnt = chunk->counter.load();

			if(cnt == MIN)
				return false;

			if(atomic_compare_exchange_strong(&(chunk->counter), cnt, cnt - 1))
				return true;
		}
	}

	bool find(Chunk *chunk, long key);

	void markChunkFrozen(Chunk *chunk) {
		// Not sure if this should be till MAX or counter
		for (int i = 0; i < MAX; i++) {
			long savedNext = (uint128) chunk->entriesArray[i].nextEntry.load();
			while (!Entry::isFrozen(savedNext)) {
				chunk->entriesArray[i].nextEntry.compare_exchange_strong(
						savedNext, (long) Entry::markFrozen(savedNext));
				savedNext = chunk->entriesArray[i].nextEntry.load();
			}

			uint128 savedWord = chunk->entriesArray[i].keyData;
			while (!Entry::isFrozen(savedWord)) {
				Utils::InterlockedCompareExchange128(
						&(chunk->entriesArray[i].keyData), savedWord,
						Entry::markFrozen(savedWord));
				savedWord = chunk->entriesArray[i].keyData;
			}
		}
	}

	void stabilizeChunk(Chunk *chunk) {
		long maxKey = LONG_MAX;
		find(chunk, maxKey);		// implicitly removes all the delete entries
		for (int i = 0; i < MAX; i++) {
			long key = chunk->entriesArray[i].getKey();
			Entry *next = chunk->entriesArray[i].nextEntry.load();
			if ((key != Utils::DEFAULT_KEY) && !Entry::isDeleted(next)) {
				if (!find(chunk, key))
					insertEntry(chunk, chunk->entriesArray[i], key);
			}
		}
	}

	RecovType freezeDecision(Chunk *chunk) {
		Entry *e = chunk->head->nextEntry.load();
		int cnt = 0;
		while (Entry::clearFrozen((long) e) != 0) {
			cnt++;
			e = e->nextEntry.load();
		}

		if (cnt == MIN)
			return MERGE;

		if (cnt == MAX)
			return SPLIT;

		return COPY;
	}

	Chunk *findMergeSlave(Chunk *master) {
		bool result;
		Chunk *slave, *expectedMergeBuddy, *newMergeBuddy;

		while (true) {
			slave = listFindPrevious(master);
			expectedMergeBuddy = combineChunkState(NULL, NO_FREEZE);
			newMergeBuddy = combineChunkState(master, EXTERNAL_FREEZE);

			if (!atomic_compare_exchange_strong(&(slave->mergeBuddy),
					expectedMergeBuddy, newMergeBuddy)) {
				if (slave->mergeBuddy == newMergeBuddy)
					break;

				// runtime crash
				freeze(slave, 0, master, ENSLAVE, &result);
			} else
				break;
		}

		markChunkFrozen(slave);
		stabilizeChunk(slave);

		expectedMergeBuddy = combineChunkState(NULL, INTERNAL_FREEZE);
		newMergeBuddy = combineChunkState(slave, INTERNAL_FREEZE);

		atomic_compare_exchange_strong(&(master->mergeBuddy),
				expectedMergeBuddy, newMergeBuddy);

		return slave;
	}

	Chunk *freezeRecovery(Chunk *oldChunk, long key, TData data,
			RecovType recovType, Chunk *mergeChunk, TriggerType trigger,
			bool *result) {
		Chunk *retChunk = NULL, newChunk2 = NULL;

		Chunk *newChunk1 = allocate();		// TODO: provide min and max

		long separateKey;

		switch (recovType) {
		case COPY:
			copyToOneChunk(oldChunk, newChunk1);
			break;

		case MERGE:
			if ((getEntrNum(oldChunk) + getEntrNum(mergeChunk)) >= MAX) {
				// Two chunks will be merged into two chunks
				newChunk2 = allocate();
				newChunk1->nextChunk = newChunk2;
				newChunk2->nextChunk = NULL;
				separateKey = mergeToTwoChunks(oldChunk, mergeChunk, newChunk1,
						newChunk2);

			} else {
				mergeToOneChunk(oldChunk, mergeChunk, newChunk1);
			}
			break;

		case SPLIT:
			newChunk2 = allocate();
			newChunk1->nextChunk = newChunk2;
			newChunk2->nextChunk = NULL;
			separateKey = splitIntoTwoChunks(oldChunk, newChunk1, newChunk2);
			break;
		}

		helpInFreezeRecovery(newChunk1, newChunk2, key, separateKey, data,
				trigger);

		if (!atomic_compare_exchange_strong(&(oldChunk->newChunk), NULL,
				newChunk1)) {
			retireChunk(newChunk1);

			if (newChunk2 != NULL)
				retireChunk(newChunk2);

			if (key < separateKey)
				retChunk = oldChunk->newChunk.load();
			else
				retChunk = findChunk(key);

		} else {
			retChunk = NULL;
		}

		listUpdate(recovType, key, oldChunk);
		return retChunk;
	}

	// A lot of changes in this method than actual
	void helpInFreezeRecovery(Chunk *newChunk1, Chunk *newChunk2, long key,
			long separateKey, TData data, TriggerType trigger) {

		switch (trigger) {
		case DELETE:
			deleteInChunk(newChunk1, key);
			if (newChunk2 != NULL)
				deleteInChunk(newChunk2, key);
			break;

		case INSERT:
			if (newChunk2 != NULL && key < separateKey)
				insertToChunk(newChunk2, key, data);
			else
				insertToChunk(newChunk1, key, data);
			break;

		case ENSLAVE:
			if (newChunk2 != NULL)
				newChunk2->mergeBuddy = combineChunkState(data,
						EXTERNAL_FREEZE);
			else
				newChunk1->mergeBuddy = combineChunkState(data,
						EXTERNAL_FREEZE);
			break;
		}
	}

	bool deleteInChunk(Chunk *chunk, long key);
	void retireChunk(Chunk *chunk);
	void listUpdate(RecovType recovType, long key, Chunk * chunk);

	Chunk *listFindPrevious(Chunk * chunk) {
		if(findChunk(chunk->head->nextEntry.load()->getKey()) != chunk)
			return chunk->mergeBuddy.load();

		return prev;
	}
};

//} /* namespace lflist */

#endif /* CHUNK_HPP_ */
