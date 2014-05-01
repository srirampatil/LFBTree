/*
 * lfbtree.hpp
 *
 *  Created on: 13-Apr-2014
 *      Author: jigar
 */

#ifndef LFBTREE_HPP_
#define LFBTREE_HPP_

#include "../lflist/Chunk.hpp"
#include "Node.hpp"

class BTree {
private:

	typedef Chunk<Node*> ChunkType;
	atomic<Node *> root;
//	static __thread EntryType *cur;
//	static __thread EntryType **prev;
//	static __thread EntryType *next;
//	static __thread Node *node;

public:
	BTree(int min, int max) {
		root = new Node(min, max);
	}

	/*
	 * Finding the relevant leaf node given a key.
	 */
	Node* FindLeaf(long key) {
		Node* node = root;
		while (node->height != 0) {		// Current node is not leaf
			node->chunk->Find(node->chunk, key); // entry’s data field is a pointer to the child node
			node = (Node*) ChunkType::cur->getData();
		}
		return node; // current node is leaf
	}
	/*
	 * 	Finds parent and slave also if slaveEnt is not null
	 */

	Node* FindParent(long key, Node* childNode, Entry** prntEnt,
			Entry ** slaveEnt) {
		Node* node = root;
		while (node->height != 0) {		// Current node is not leaf
			node->chunk->Find(node->chunk, key);
			if (childNode == (Node*) ChunkType::cur->getData()) {// Check whether we found exactly the entry that points to the child
				*prntEnt = ChunkType::cur;
				if (slaveEnt != NULL) {	// Look for the child’s neighbor, check if the current entry is the leftmost
					if (ChunkType::prev == &(node->chunk->head))
						*slaveEnt = ChunkType::next;
					else
						*slaveEnt = EntPtr(ChunkType::prev);
				} 	// end of if child neighbor was needed
				if (node->getFreezeState() == INFANT)
					helpInfant(node);	// Help infant parent node
				return node;
			} 	// end of if child was found
			node = (Node*) ChunkType::cur->getData();

		} 	// end of while current node is not leaf
		return NULL;	// Current node is leaf, no parent found
	}

	//TODO **prev pointer points to the inner field next of the entry that precedes the entry pointed to by *cur. We use EntPtr() to
	//	convert it to a normal pointer to the previous entry.
	Entry* EntPtr(Entry **prev) {

	}

	bool SearchInBtree(long key, Node *data) {
		Node * node = FindLeaf(key);
		return node->chunk->SearchInChunk(node->chunk, key, &data);
	}

	bool InsertToBtree(long key, Node data) {
		Node* node = FindLeaf(key);
		if (node->getFreezeState() == INFANT)
			helpInfant(node);
		return node->chunk->InsertToChunk(node->chunk, key, &data);
	}

	bool DeleteFromBtree(long key, Node data) {
		Node* node = FindLeaf(key);
		if (node->getFreezeState() == INFANT)
			helpInfant(node);
		return node->chunk->DeleteInChunk(node->chunk, key);
	}
	/*
	 *
	 * @sepKey sepKey is the highest key in the low-values new node
	 */
	void InsertSplitNodes(Node* node, long sepKey) {

		Entry *nodeEnt;	// Pointer to the parent’s entry pointing to the node about to be split
		Node* n1 = node->newNode;// Pointer to the new node that holds the lower keys
		Node* n2 = node->newNode->nextNew;// Pointer to the new node that holds the higher keys
		Node* parent;
		long maxKey = getMaxKey(node);

		if ((parent = FindParent(sepKey, node, &nodeEnt, NULL)) != NULL) {
			parent->chunk->InsertToChunk(parent->chunk, sepKey, n1);// Can only fail if someone else completes it before we do
		}
		if ((parent = FindParent(maxKey, node, &nodeEnt, NULL)) != NULL) {
			long oldKeyData = nodeEnt->combineKeyData(nodeEnt->getKey(), node);
			long newKeyData = nodeEnt->combineKeyData(nodeEnt->getKey(), n2);
			parent->chunk->replaceInChunk(parent->chunk, nodeEnt->getKey(),
					oldKeyData, newKeyData);
		}		// Update the states of the new nodes from INFANT to NORMAL .

		n1->compareAndSetJionBuddy(INFANT, 0, NORMAL, 0);
		n2->compareAndSetJionBuddy(INFANT, 0, NORMAL, 0);
//		CAS(&(n1-><freezeState, joinBuddy>), < INFANT, NULL>, <NORMAL, NULL>);
//		CAS(&(n2-><freezeState, joinBuddy>), < INFANT, NULL>, <NORMAL, NULL>);

		return;
	}

	Node* FindJoinSlave(Node* master) {
		Node* oldSlave = NULL;
		Node* parent;
		Node* slave;
		Entry *slaveEnt;
		Entry *masterEnt;
		bool result;
		long expState;

		start: Entry* nextEntry =
				(Entry*) master->chunk->head->nextEntry.load();
		long anyKey = nextEntry->getKey();	// Obtain an arbitrary master key
		if ((parent = FindParent(anyKey, master, &masterEnt, &slaveEnt)) == NULL)// If master is not in the B+tree;
			return (Node*) master->joinBuddy.load();// thus its slave was found and is written in the joinBuddy
		slave = (Node*) slaveEnt->getData();// Slave candidate found in the tree
		// Set master’s freeze state to <REQUEST SLAVE, slave>; oldSlave is not NULL if the code is repeated
		if (oldSlave == NULL)
			expState = ((long) NULL | FREEZE);
		else
			expState = ((long) oldSlave | REQUEST_SLAVE);
		if (!master->compareAndSetJionBuddy(expState, REQUEST_SLAVE,
				(long) slave)) {// Master’s freeze state can be only REQUEST SLAVE, JOIN or SLAVE FREEZE if the roles were swaped
			if (master->getFreezeState() == JOIN)
				return (Node*) master->joinBuddy.load();
		}
		slave = (Node*) master->joinBuddy.load();// Current slave is the one pointed by joinBuddy	// Check that parent is not in a frozen state and help frozen parent if needed
		if ((parent->getFreezeState() != NORMAL) && (oldSlave == NULL)) {
			Freeze(parent, 0, 0, master, NONE, &result);
			oldSlave = slave;
			goto start;
		}
		if (!SetSlave(master, slave, anyKey,
				((Entry*) slave->chunk->head->nextEntry.load())->getKey())) {
			oldSlave = slave;
			goto start;
		}	// We succeed to get the slave update master
		if (master->getFreezeState() == JOIN)
			return slave;
		else
			return NULL;
	}
	bool SetSlave(Node* master, Node* slave, long masterKey, long slaveKey) {
		bool result;
		while (!slave->compareAndSetJionBuddy(NORMAL, 0, SLAVE_FREEZE,
				(long) master)) {
			// Help slave, different helps for frozen slave and infant slave
			if (slave->getFreezeState() == INFANT) {
				helpInfant(slave);
				return false;
			} else if (slave->compareJionBuddy(SLAVE_FREEZE, (long) master))
				break;
			else {
				// The slave is under some kind of freeze, help and look for new slave
				// Check for a special case: two leftmost nodes try to enslave each other, break the symmetry
				if (slave->compareJionBuddy(REQUEST_SLAVE, (long) master)) {
					if (masterKey < slaveKey) {
						// Current master node is left sibling and should become a slave
						if (master->compareAndSetJionBuddy(REQUEST_SLAVE,
								(long) slave, SLAVE_FREEZE, (long) slave))
							return true;
						else
							return false;
					} else {
						// Current master node is right sibling and the other node should become a slave
						if (slave->compareAndSetJionBuddy(REQUEST_SLAVE,
								(long) master, SLAVE_FREEZE, (long) master))
							return true;
						else
							return false;
					}
				}

				// end case of two leftmost nodes trying to enslave each other
				Freeze(slave, 0, NULL, master, ENSLAVE, &result);
				// Help in different freeze activity
				return false;
			} // end of investigating the enslaving failure
		} // end of while
		slave->chunk->markChunkFrozen(slave->chunk);
		slave->chunk->stabilizeChunk(slave->chunk); // Slave enslaved successfully. Freeze the slave
		return true;
	}

	void InsertMergeNode(Node* master) {
		Node* newNode = master->newNode;
		Node* slave = (Node*) master->joinBuddy.load();
		long maxMasterKey = getMaxKey(master);
		long maxSlaveKey = getMaxKey(slave);
		long highKey, lowKey, highEntKey;
		Node *highNode, *lowNode, *parent;
		Entry *highEnt, *lowEnt;
		// Both nodes are frozen
		if (maxSlaveKey < maxMasterKey) {
			// Find low and high keys among master and slave
			highKey = maxMasterKey;
			highNode = master;
			lowKey = maxSlaveKey;
			lowNode = slave;
		} else {
			highKey = maxSlaveKey;
			highNode = slave;
			lowKey = maxMasterKey;
			lowNode = master;
		}
		if ((parent = FindParent(highKey, highNode, &highEnt, NULL)) != NULL) {
			highEntKey = highEnt->getKey();
			// Change the highest key entry to point on new node
			parent->chunk->replaceInChunk(parent->chunk, highEntKey,
					combine(highEntKey, highNode),
					combine(highEntKey, newNode));
			// continue anyway
		}
		// If high node cannot be found continue to the low
		if ((parent = FindParent(lowKey, lowNode, &lowEnt, NULL)) != NULL) {
			if (parent->root)
				MergeRoot(parent, newNode, lowNode, lowEnt->getKey());
			else
				parent->chunk->deleteInChunk(parent->chunk, lowEnt->getKey(),
						lowNode);

			// lowNode is the expected data
		}
// If also low node can no longer be found on the tree, then the merge was completed (by someone else).
// Try to update the new node state from INFANT to NORMAL.
		newNode->compareAndSetJionBuddy(INFANT, 0, NORMAL, 0);
		return;
	}

	void InsertBorrowNodes(Node* master, long sepKey) { // sepKey is the highest key in the low-values new node
		Node* n1 = master->newNode;
		// Pointer to the new node that holds the lower keys
		Node* n2 = master->newNode->nextNew;
		// Pointer to the new node that holds the higher keys
		Node* slave = (Node*) master->joinBuddy.load();
		long maxMasterKey = getMaxKey(master);
		long maxSlaveKey = getMaxKey(slave);
		long highKey, lowKey;
		Node* oldHigh;
		Node* oldLow, *sepKeyNode;
		if (maxSlaveKey < maxMasterKey) {
			highKey = maxMasterKey;
			oldHigh = master;
			lowKey = maxSlaveKey;
			oldLow = slave;
		} else {
			highKey = maxSlaveKey;
			oldHigh = slave;
			lowKey = maxMasterKey;
			oldLow = master;
		}
		if (lowKey < sepKey)
			sepKeyNode = oldHigh;

		Node * insertParent;
		Entry * ent;
		if ((insertParent = FindParent(sepKey, sepKeyNode, &ent, NULL)) != NULL) {
			insertParent->chunk->insertToChunk(insertParent->chunk, sepKey, n1);
			// Insert reference to the new node with the lower keys
		}
		Node* highParent, *lowParent;
		Entry * highEnt;
		if ((highParent = FindParent(highKey, oldHigh, &highEnt, NULL)) != NULL) {
			// Find the parent of the old node
			highParent->chunk->replaceInChunk(highParent->chunk,
					highEnt->getKey(), combine(highEnt->key, oldHigh),
					combine(highEnt->key, n2));
			// node with the higher keys
		}
		Entry * lowEnt;
		if ((lowParent = FindParent(lowKey, oldLow, &lowEnt, NULL)) != NULL) {
			// Delete, currently duplicated,
			lowParent->chunk->deleteInChunk(lowParent->chunk, lowEnt->getKey(),
					oldLow);
			// reference to the old low node
		}
		// Try to update the new children states to NORMAL from INFANT
		n1->compareAndSetJionBuddy(INFANT,0, NORMAL,0);
		n2->compareAndSetJionBuddy(INFANT,0, NORMAL,0);
		return;
	}

	void CallForUpdate(FreezeState freezeState, Node* node, long sepKey) {

		Node* n1 = node->newNode;
		Node* n2 = node->newNode->nextNew;
		Entry* nodeEnt;
		Node* parent;
		bool False = false;
		switch (freezeState) {
		case COPY:
			if (node->root.load()) {
				node->root.compare_exchange_strong(False, true);
				root.compare_exchange_strong(node, n1);
			} else if ((parent = FindParent(
					((Entry*) node->chunk->head->nextEntry.load())->getKey(),
					node, &nodeEnt, NULL)) != NULL) {
				parent->chunk->replaceInChunk(parent->chunk,
						combine(nodeEnt->getKey(), node),
						combine(nodeEnt->getKey(), n1));
			}

			n1->compareAndSetFreezeState(INFANT, NORMAL);
			return;
		case SPLIT:
			 if ( node->root )
				 SplitRoot(node, sepKey, n1, n2);
			 else
				 InsertSplitNodes(node, sepKey);
			return;
		case JOIN:
			if(n2 == NULL)
				InsertMergeNode(node);
			else
				InsertBorrowNodes(node,sepKey);
			return;
		}
	}

	void helpInfant(Node* node){
	 Node* creator = node->creator;
	 FreezeState creatorFrSt = (FreezeState)creator->getFreezeState();
	 Node* n1 = creator->newNode;
	 Node* n2 = creator->newNode->nextNew;
	 long sepKey = getMaxKey(n1);

	 if(n1->getFreezeState() != INFANT){
		 if(n2!=NULL){
			 n1->compareAndSetJionBuddy(INFANT,0,NORMAL,0);
		 }
	 }

	 if ( (creator->root.load()) && (creatorFrSt == SPLIT ) ) { // If this is root split only children’s state correction is needed
		 n1->compareAndSetJionBuddy(INFANT,0,NORMAL,0);
		 n2->compareAndSetJionBuddy(INFANT,0,NORMAL,0);
		 return;
	 }
	 switch ( creatorFrSt ) {
	  case COPY :
		  node->compareAndSetJionBuddy(INFANT,0,NORMAL,0);
		  return;
	  case SPLIT:
		  InsertSplitNodes(creator, sepKey); return;
	  case JOIN:
		  if (n2 == NULL)
			  InsertMergeNode(creator); // If freeze state is JOIN and there is one new node, help in merge
		  else
			  InsertBorrowNodes(creator, sepKey);
		  return;
	  }
	}

	void SplitRoot (Node* oldRoot, long sepKey, Node* n1, Node* n2) {
		Node* newRoot = new Node();	 // Allocate new root with freeze state set to INFANT
		newRoot->setFreezeState(NORMAL);
		newRoot->root.store(true);
		newRoot->height = oldRoot->height+1;
		//TODO addrootsons
		addRootSons(newRoot, sepKey, n1, ∞, n2);
		root.compare_exchange_strong(oldRoot,newRoot);
		// Try to replace the old root pointer with the new
		// If CAS is unsuccessful, then old root’s new nodes were inserted under other new root,
		n1->compareAndSetJionBuddy(INFANT,0,NORMAL,0);
		n2->compareAndSetJionBuddy(INFANT,0,NORMAL,0);
		return;
	}


	void MergeRoot (Node* root, Node* posibleNewRoot, Node* c1, c1Key) {
	 rootEntNum=GetEntNum(root→chunk,&firstEnt,&secondEnt); // Count the entries in the list (do not use counter)
	 if( rootEntNum > 2 ) { DeleteInChunk(&(root→chunk), c1Key, c1); return; }
	 // rootEntNum is 2 here, check that first entry points to the frozen low node second on infant new possible root
	 if((firstEnt→data == c1) && (secondEnt→data == posibleNewRoot)) {
	 CAS(&(posibleNewRoot→root), 0, 1);
	 // Mark as root
	 CAS(&(btree→root), root, posibleNewRoot);
	 // Try to replace the old root pointer with the new
	 // If CAS is unsuccessful, then old root was changed by someone else
	 }
	 return;
	}

	Chunk<Node*>* Freeze(Node* node, long key, Node* expected, Node *data,
			TriggerType tgr, bool* res);
	long getMaxKey(Node* node);
};

#endif /* LFBTREE_HPP_ */
