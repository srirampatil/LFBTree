/*
 * Node.hpp
 *
 *  Created on: 13-Apr-2014
 *      Author: jigar
 */






#ifndef NODE_HPP_
#define NODE_HPP_

#include "../lflist/Chunk.hpp"


//enum FreezeState {
//
//};

class Node{
	public:
		int height;
		Chunk<Node*> *chunk;
		std::atomic<bool> root;
		Node* newNode;
		Node* nextNew;
		Node* creator;
		std::atomic<long> joinBuddy;
		Node(){
		}
		Node(int min,int max){
			chunk = new Chunk<Node*>(min,max);
		}
		virtual ~Node(){
			setFreezeState(INFANT);
		}

		bool compareAndSetJionBuddy(FreezeState oldFreezeState,long oldJoinBuddy,FreezeState newFreezeState,long newJoinBuddy){
			oldJoinBuddy = ( oldJoinBuddy | oldFreezeState);
			newJoinBuddy = ( newJoinBuddy | newFreezeState);
			return joinBuddy.compare_exchange_strong(oldJoinBuddy,newJoinBuddy);
		}
		bool compareAndSetJionBuddy(long oldJoinBuddy,FreezeState newFreezeState,long newJoinBuddy){
			newJoinBuddy = ( newJoinBuddy | newFreezeState);
			return joinBuddy.compare_exchange_strong(oldJoinBuddy,newJoinBuddy);
		}

		int	getFreezeState() {
			return (long) joinBuddy.load() & 0x00000000000000000000000000000007;
		}

		bool compareJionBuddy(FreezeState newFreezeState,long newJoinBuddy){
			long newJoinBuddy1 = (newJoinBuddy | newFreezeState);
			return newJoinBuddy1 == joinBuddy;

		}
		bool compareAndSetFreezeState(FreezeState oldFreezeState,FreezeState newFreezeState){
			long newJoinBuddy = (joinBuddy | newFreezeState);
			long oldJoinBuddy = (joinBuddy | oldFreezeState);
			return joinBuddy.compare_exchange_strong(oldJoinBuddy,newJoinBuddy);
		}

		void setFreezeState(FreezeState frz){
			joinBuddy.store((joinBuddy.load() & 0xfffffffffffffffffffffffffffffff8) | frz);
		}

};




#endif /* NODE_HPP_ */
