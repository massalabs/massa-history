# Indexer V2

NOTE: THIS IS A "GENERAL BROAD IDEA", PLEASE BRAINSTORM SOMETHING BETTER AND SMARTER.

First read carefully https://docs.massa.net as well as the source code of the massa node in the massa/ folder.
The GRPC/protobuf interface is in massa-proto/

## Goal

The goal of the indexer V2 is to improve upon the existing Massa indexer and move away from AWS to reduce costs.
Eventually we want to build an explorer that consumes the API of those 

## General topology

There will be 3 servers for the indexer, il full redundancy (if one or 2 die then reset or resume, we can resume).
Each one will be:
* running its own Massa node compiled with the right arguments so it streams everything needed
* running its own rust-written Indexer instance that connects to the local node streams, and storing its own rocksdb database locally, and exposing the indexer public API
The three indexers should remain in sync and backfill on each other any issues. Forks and conflicts must be handled in a smart way.

## Indexer streaming logic

The indexer needs to connect to the following GRPC streams in the node:
    * NewFilledBlocksServer: gives a stream of all blocks arriving in the node
    * NewSlotExecutionOutputsServer: gives info on the execution status of slots (speculative execution, outcome of operation executions etc...)
    * NewTransfersInfoServer: once slots are finalized, this gives a report on all native coin transfers that happened


Everytime a new block arrives from NewFilledBlocksServer, we the indexer adds it to its indexed database, together with the operations, endorsements, denunciations it contains.

Whenever NewSlotExecutionOutputsServer signals that a slot was executed either speculatively or finally:
* if the execution is speculative, simply mark the slot, update the block's and operations/denunciations/endorsements it contains execution status (together with the outcome of the execution, eg. success or failure, assuming the operation was executed at all, sometimes the tx might just not be executed despite being in a block, be careful about that). Multiple speculative executions of the same slot might occur. If a slot is re-executed, reset the status of all the slots AFTER it as unexecuted, because history is rewinded.
* if the execution of a slot is final:
    * mark the status of the block that was eventually finalized + operations/denunciations/endorsements to their final execution status
    * delete all the transactions that expired at the latest final block and were never included in any block
    * also forget about any blocks that we know about that are at a slot before (inclusive) the latest finalized slot (cleanup their unused dependencies) but not executed

Whenver a NewTransfersInfoServer item arrives, index all the coin transfers for that slot. Note that it only emits at finalization.

Be smart, support deferred calls / async messages as well.


## Backfill logic

Each block has either 32 parents (1 per thread) or no parents (genesis blocks).
Note that in case of desync/fork, different indexers might have different block hashes or execution results for the same final slot.
Some slots can contain no block (block miss), that's OK, but they might still execute some stuff through autonomous SCs, deferred calls, deferred credits etc...
The system needs to launch a separate thread that detects and fills gaps in the final slots.
For example, if we see that we have info on a final block/slot but we are missing info on one of the parent blocks/slots, the indexer should try to gather as much data as possible step by step in the backward direction (from child to parents) by querying info from the other indexers (if one of them does not have the data or does not respond, try another one until you have tried them all).
If there is a gap after backfill wehre none of the nodes have info, don't stop the backfill there, just interrogate the other nodes for slots going backwards one by one on the slot (it should give you the block ID that is there, if any), and take the majority vote (or a random choice if only 2 respond in a contradictory way) on the block ID (if any, there can be miss) to retrieve for that slot.
But query only if what we have in our db somehow does not match the backfill (eg. the block hash it mentions for a slot does not match the parents claimed by the local node currently) ! Otherwise, the local db has priority.


Note that if you end up replacing an existing slot in the db, don't forget to cleanup any dependencies of the previously stored slot as long as they are not still in use.

## Indexer database format

Timestamps can be deduced deterministically from slot (period, thread), see code.

The database must be persistant, minimize space usage and allow for all the following functions:
* retrieve a full operation, block, endorsement or denunciation by its ID
* retrieve the latest info of a slot execution by providing the slot (period, thread):
    * execution status
    * block ID if any
    * ooutcome of the various executions that happened at the slot (in particular the operations/denunciations/endorsements/async calls/deferred calls that were executed or not, and their execution outcome)
    * list of transfers if available with transfer info
* for each block be able to efficiently:
    * retrieve it fully by slot number (under speculative (nn-final) status, there might be multiple blocks at the same slot)
* for each operation, endorsement, denunciation, deferred call, async message:
    * be able to retrieve it efficiently by ID, slot number, by emitter address, target of a coin transfer, target of a call, including block ID if any
* for each transfer, also index it by the sender/receiver/slot 

Also sort stuff by the date linked to the slot of its first block inclusion. This is useful for example for stuff like "list the latest coin transfers that are linked to my address") 


## Indexer API

Make an API that satisfies all the needs of a fully featured blockchain explorer for massa.

Note that the API should also forward and cache some direct node requests (eg. get the current balance of an address, its number of rolls etc... see QueryStateRequest)

Also something to list the current stakers etc... see GRPC spec of the node

# Explorer frontend

ALSO BUILD A FRONTEND (for now running locally) EXPLORER THAT REPRESENTS A FULL BLOCK EXPLORER CONSUMING THE API AND THAT I CAN TRY.





AGAIN, THIS IS JUST TO GIVE YOU AN IDEA, MAKE IT A VERY COOL AND FULL FEATURED/ROBUST EXPLORER SYSTEM FOR MASSA


Use GRPC for indexer-indexer communication and for indexer-node communication.
Use REST for communication with the front-end.