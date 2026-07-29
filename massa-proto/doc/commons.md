# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [massa/model/v1/datastore.proto](#massa_model_v1_datastore-proto)
    - [AddressKeyEntry](#massa-model-v1-AddressKeyEntry)
    - [AddressKeysEntries](#massa-model-v1-AddressKeysEntries)
    - [DatastoreEntry](#massa-model-v1-DatastoreEntry)
  
- [massa/model/v1/staker.proto](#massa_model_v1_staker-proto)
    - [StakerEntry](#massa-model-v1-StakerEntry)
  
- [massa/model/v1/draw.proto](#massa_model_v1_draw-proto)
    - [EndorsementDraw](#massa-model-v1-EndorsementDraw)
    - [SlotDraw](#massa-model-v1-SlotDraw)
  
- [massa/model/v1/versioning.proto](#massa_model_v1_versioning-proto)
    - [MipComponentEntry](#massa-model-v1-MipComponentEntry)
    - [MipInfo](#massa-model-v1-MipInfo)
    - [MipStatusEntry](#massa-model-v1-MipStatusEntry)
  
    - [ComponentStateId](#massa-model-v1-ComponentStateId)
    - [MipComponent](#massa-model-v1-MipComponent)
  
- [massa/model/v1/stats.proto](#massa_model_v1_stats-proto)
    - [ConsensusStats](#massa-model-v1-ConsensusStats)
    - [ExecutionStats](#massa-model-v1-ExecutionStats)
    - [NetworkStats](#massa-model-v1-NetworkStats)
    - [PoolStats](#massa-model-v1-PoolStats)
  
- [massa/model/v1/node.proto](#massa_model_v1_node-proto)
    - [CompactConfig](#massa-model-v1-CompactConfig)
    - [ConnectedNode](#massa-model-v1-ConnectedNode)
    - [NodeStatus](#massa-model-v1-NodeStatus)
    - [PublicStatus](#massa-model-v1-PublicStatus)
  
    - [ConnectionType](#massa-model-v1-ConnectionType)
  
- [massa/model/v1/operation.proto](#massa_model_v1_operation-proto)
    - [CallSC](#massa-model-v1-CallSC)
    - [ExecuteSC](#massa-model-v1-ExecuteSC)
    - [OpTypes](#massa-model-v1-OpTypes)
    - [Operation](#massa-model-v1-Operation)
    - [OperationIds](#massa-model-v1-OperationIds)
    - [OperationInfo](#massa-model-v1-OperationInfo)
    - [OperationType](#massa-model-v1-OperationType)
    - [OperationTypeRoll](#massa-model-v1-OperationTypeRoll)
    - [OperationWrapper](#massa-model-v1-OperationWrapper)
    - [RollBuy](#massa-model-v1-RollBuy)
    - [RollSell](#massa-model-v1-RollSell)
    - [SignedOperation](#massa-model-v1-SignedOperation)
    - [Transaction](#massa-model-v1-Transaction)
  
    - [OpType](#massa-model-v1-OpType)
  
- [massa/model/v1/denunciation.proto](#massa_model_v1_denunciation-proto)
    - [BlockHeaderDenunciation](#massa-model-v1-BlockHeaderDenunciation)
    - [Denunciation](#massa-model-v1-Denunciation)
    - [DenunciationAddress](#massa-model-v1-DenunciationAddress)
    - [DenunciationBlockHeader](#massa-model-v1-DenunciationBlockHeader)
    - [DenunciationEndorsement](#massa-model-v1-DenunciationEndorsement)
    - [DenunciationIndex](#massa-model-v1-DenunciationIndex)
    - [EndorsementDenunciation](#massa-model-v1-EndorsementDenunciation)
  
- [massa/model/v1/commons.proto](#massa_model_v1_commons-proto)
    - [ArrayOfBytesWrapper](#massa-model-v1-ArrayOfBytesWrapper)
    - [BytesMapFieldEntry](#massa-model-v1-BytesMapFieldEntry)
    - [Empty](#massa-model-v1-Empty)
    - [Error](#massa-model-v1-Error)
    - [KeyPair](#massa-model-v1-KeyPair)
  
    - [ComparisonResult](#massa-model-v1-ComparisonResult)
  
- [massa/model/v1/amount.proto](#massa_model_v1_amount-proto)
    - [NativeAmount](#massa-model-v1-NativeAmount)
  
- [massa/model/v1/execution.proto](#massa_model_v1_execution-proto)
    - [AsyncMessage](#massa-model-v1-AsyncMessage)
    - [AsyncMessageExecution](#massa-model-v1-AsyncMessageExecution)
    - [AsyncMessageTrigger](#massa-model-v1-AsyncMessageTrigger)
    - [AsyncMessageUpdate](#massa-model-v1-AsyncMessageUpdate)
    - [AsyncPoolChangeEntry](#massa-model-v1-AsyncPoolChangeEntry)
    - [AsyncPoolChangeValue](#massa-model-v1-AsyncPoolChangeValue)
    - [BytecodeExecution](#massa-model-v1-BytecodeExecution)
    - [DeferredCallExecution](#massa-model-v1-DeferredCallExecution)
    - [ExecTransferInfo](#massa-model-v1-ExecTransferInfo)
    - [ExecutedOpsChangeEntry](#massa-model-v1-ExecutedOpsChangeEntry)
    - [ExecutedOpsChangeValue](#massa-model-v1-ExecutedOpsChangeValue)
    - [ExecutionOutput](#massa-model-v1-ExecutionOutput)
    - [ExecutionStackElement](#massa-model-v1-ExecutionStackElement)
    - [FinalizedExecutionOutput](#massa-model-v1-FinalizedExecutionOutput)
    - [FunctionCall](#massa-model-v1-FunctionCall)
    - [LedgerChangeEntry](#massa-model-v1-LedgerChangeEntry)
    - [LedgerChangeValue](#massa-model-v1-LedgerChangeValue)
    - [LedgerEntry](#massa-model-v1-LedgerEntry)
    - [LedgerEntryUpdate](#massa-model-v1-LedgerEntryUpdate)
    - [ReadOnlyExecutionCall](#massa-model-v1-ReadOnlyExecutionCall)
    - [ReadOnlyExecutionOutput](#massa-model-v1-ReadOnlyExecutionOutput)
    - [ScExecutionEvent](#massa-model-v1-ScExecutionEvent)
    - [ScExecutionEventContext](#massa-model-v1-ScExecutionEventContext)
    - [ScExecutionEventsStatus](#massa-model-v1-ScExecutionEventsStatus)
    - [SetOrDeleteDatastoreEntry](#massa-model-v1-SetOrDeleteDatastoreEntry)
    - [SetOrKeepAsyncMessageTrigger](#massa-model-v1-SetOrKeepAsyncMessageTrigger)
    - [SetOrKeepBalance](#massa-model-v1-SetOrKeepBalance)
    - [SetOrKeepBool](#massa-model-v1-SetOrKeepBool)
    - [SetOrKeepBytes](#massa-model-v1-SetOrKeepBytes)
    - [SetOrKeepSlot](#massa-model-v1-SetOrKeepSlot)
    - [SetOrKeepString](#massa-model-v1-SetOrKeepString)
    - [SetOrKeepUint64](#massa-model-v1-SetOrKeepUint64)
    - [SlotExecutionOutput](#massa-model-v1-SlotExecutionOutput)
    - [StateChanges](#massa-model-v1-StateChanges)
    - [TargetAmount](#massa-model-v1-TargetAmount)
    - [TransferValue](#massa-model-v1-TransferValue)
  
    - [AsyncPoolChangeType](#massa-model-v1-AsyncPoolChangeType)
    - [CoinOrigin](#massa-model-v1-CoinOrigin)
    - [ExecutionOutputStatus](#massa-model-v1-ExecutionOutputStatus)
    - [LedgerChangeType](#massa-model-v1-LedgerChangeType)
    - [OperationExecutionStatus](#massa-model-v1-OperationExecutionStatus)
    - [ScExecutionEventStatus](#massa-model-v1-ScExecutionEventStatus)
  
- [massa/model/v1/slot.proto](#massa_model_v1_slot-proto)
    - [IndexedSlot](#massa-model-v1-IndexedSlot)
    - [Slot](#massa-model-v1-Slot)
    - [SlotRange](#massa-model-v1-SlotRange)
    - [Slots](#massa-model-v1-Slots)
  
- [massa/model/v1/address.proto](#massa_model_v1_address-proto)
    - [Addresses](#massa-model-v1-Addresses)
    - [NativeAddress](#massa-model-v1-NativeAddress)
  
    - [AddressCategory](#massa-model-v1-AddressCategory)
  
- [massa/model/v1/block.proto](#massa_model_v1_block-proto)
    - [Block](#massa-model-v1-Block)
    - [BlockHeader](#massa-model-v1-BlockHeader)
    - [BlockIds](#massa-model-v1-BlockIds)
    - [BlockInfo](#massa-model-v1-BlockInfo)
    - [BlockParent](#massa-model-v1-BlockParent)
    - [BlockWrapper](#massa-model-v1-BlockWrapper)
    - [FilledBlock](#massa-model-v1-FilledBlock)
    - [FilledOperationEntry](#massa-model-v1-FilledOperationEntry)
    - [SignedBlock](#massa-model-v1-SignedBlock)
    - [SignedBlockHeader](#massa-model-v1-SignedBlockHeader)
  
    - [BlockStatus](#massa-model-v1-BlockStatus)
  
- [massa/model/v1/endorsement.proto](#massa_model_v1_endorsement-proto)
    - [Endorsement](#massa-model-v1-Endorsement)
    - [EndorsementIds](#massa-model-v1-EndorsementIds)
    - [EndorsementInfo](#massa-model-v1-EndorsementInfo)
    - [EndorsementWrapper](#massa-model-v1-EndorsementWrapper)
    - [SignedEndorsement](#massa-model-v1-SignedEndorsement)
  
- [massa/model/v1/time.proto](#massa_model_v1_time-proto)
    - [NativeTime](#massa-model-v1-NativeTime)
  
- [Scalar Value Types](#scalar-value-types)



<a name="massa_model_v1_datastore-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## massa/model/v1/datastore.proto



<a name="massa-model-v1-AddressKeyEntry"></a>

### AddressKeyEntry
AddressKeyEntry


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Associated address of the entry |
| key | [bytes](#bytes) |  | Datastore key |






<a name="massa-model-v1-AddressKeysEntries"></a>

### AddressKeysEntries
AddressKeys holds a list of addresses - keys


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address_key_entries | [AddressKeyEntry](#massa-model-v1-AddressKeyEntry) | repeated | List of address- key entries |






<a name="massa-model-v1-DatastoreEntry"></a>

### DatastoreEntry
DatastoreEntry


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| final_value | [bytes](#bytes) |  | final datastore entry value |
| candidate_value | [bytes](#bytes) |  | candidate_value datastore entry value |





 

 

 

 



<a name="massa_model_v1_staker-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## massa/model/v1/staker.proto



<a name="massa-model-v1-StakerEntry"></a>

### StakerEntry
StakerEntry


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address |
| rolls | [uint64](#uint64) |  | Rolls |





 

 

 

 



<a name="massa_model_v1_draw-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## massa/model/v1/draw.proto



<a name="massa-model-v1-EndorsementDraw"></a>

### EndorsementDraw
Endorsement draw


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index | [uint64](#uint64) |  | Endorsement index |
| producer | [string](#string) |  | Producer address |






<a name="massa-model-v1-SlotDraw"></a>

### SlotDraw
Slot draw


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot | [Slot](#massa-model-v1-Slot) |  | Slot |
| block_producer | [google.protobuf.StringValue](#google-protobuf-StringValue) |  | Block producer address (Optional) |
| endorsement_draws | [EndorsementDraw](#massa-model-v1-EndorsementDraw) | repeated | Endorsement draws |





 

 

 

 



<a name="massa_model_v1_versioning-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## massa/model/v1/versioning.proto



<a name="massa-model-v1-MipComponentEntry"></a>

### MipComponentEntry
MipComponentEntry


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kind | [MipComponent](#massa-model-v1-MipComponent) |  | Kind |
| version | [uint32](#uint32) |  | Version |






<a name="massa-model-v1-MipInfo"></a>

### MipInfo
MIP info (name &amp; versions &amp; time range for a MIP)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | MIP name or descriptive name |
| version | [uint32](#uint32) |  | Network (or global) version (to be included in block header) |
| start | [NativeTime](#massa-model-v1-NativeTime) |  | A timestamp at which the version gains its meaning (e.g. announced in block header) |
| timeout | [NativeTime](#massa-model-v1-NativeTime) |  | A timestamp at the which the deployment is considered failed |
| activation_delay | [NativeTime](#massa-model-v1-NativeTime) |  | Once deployment has been locked, wait for this duration before deployment is considered active |
| components | [MipComponentEntry](#massa-model-v1-MipComponentEntry) | repeated | Components concerned by this versioning (e.g. a new Block version), and the associated component_version |






<a name="massa-model-v1-MipStatusEntry"></a>

### MipStatusEntry
Entry for GetMipStatusResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mip_info | [MipInfo](#massa-model-v1-MipInfo) |  | Mip info |
| state_id | [ComponentStateId](#massa-model-v1-ComponentStateId) |  | State id |





 


<a name="massa-model-v1-ComponentStateId"></a>

### ComponentStateId
State machine for a Versioning component that tracks the deployment state

| Name | Number | Description |
| ---- | ------ | ----------- |
| COMPONENT_STATE_ID_UNSPECIFIED | 0 | Default enum value |
| COMPONENT_STATE_ID_ERROR | 1 | Error state |
| COMPONENT_STATE_ID_DEFINED | 2 | Initial state |
| COMPONENT_STATE_ID_STARTED | 3 | Past start, can only go to LockedIn after the threshold is above a given value |
| COMPONENT_STATE_ID_LOCKEDIN | 4 | Locked but wait for some time before going to active (to let users the time to upgrade) |
| COMPONENT_STATE_ID_ACTIVE | 5 | After LockedIn, deployment is considered successful (after activation delay) |
| COMPONENT_STATE_ID_FAILED | 6 | Past the timeout, if LockedIn is not reach |



<a name="massa-model-v1-MipComponent"></a>

### MipComponent
Versioning component enum

| Name | Number | Description |
| ---- | ------ | ----------- |
| MIP_COMPONENT_UNSPECIFIED | 0 | Default enum value |
| MIP_COMPONENT_ADDRESS | 1 | Address component |
| MIP_COMPONENT_KEYPAIR | 2 | Keypair component |


 

 

 



<a name="massa_model_v1_stats-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## massa/model/v1/stats.proto



<a name="massa-model-v1-ConsensusStats"></a>

### ConsensusStats
Consensus statistics


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| start_timespan | [NativeTime](#massa-model-v1-NativeTime) |  | Start of the time span for stats |
| end_timespan | [NativeTime](#massa-model-v1-NativeTime) |  | End of the time span for stats |
| final_block_count | [uint64](#uint64) |  | Number of final blocks |
| stale_block_count | [uint64](#uint64) |  | Number of stale blocks in memory |
| clique_count | [uint64](#uint64) |  | Number of actives cliques |






<a name="massa-model-v1-ExecutionStats"></a>

### ExecutionStats
Execution statistics


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| time_window_start | [NativeTime](#massa-model-v1-NativeTime) |  | Time window start |
| time_window_end | [NativeTime](#massa-model-v1-NativeTime) |  | Time window end |
| final_block_count | [uint64](#uint64) |  | Number of final blocks in the time window |
| final_executed_operations_count | [uint64](#uint64) |  | Number of final executed operations in the time window |






<a name="massa-model-v1-NetworkStats"></a>

### NetworkStats
Network statistics


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| in_connection_count | [uint64](#uint64) |  | In connections count |
| out_connection_count | [uint64](#uint64) |  | Out connections count |
| known_peer_count | [uint64](#uint64) |  | Total known peers count |
| banned_peer_count | [uint64](#uint64) |  | Banned node count |
| active_node_count | [uint64](#uint64) |  | Active node count |






<a name="massa-model-v1-PoolStats"></a>

### PoolStats
Pool statistics


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| endorsements_count | [uint64](#uint64) |  | Endorsements |
| operations_count | [uint64](#uint64) |  | Operations |





 

 

 

 



<a name="massa_model_v1_node-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## massa/model/v1/node.proto



<a name="massa-model-v1-CompactConfig"></a>

### CompactConfig
Compact configuration


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| genesis_timestamp | [NativeTime](#massa-model-v1-NativeTime) |  | Time in milliseconds when the blockclique started. |
| end_timestamp | [NativeTime](#massa-model-v1-NativeTime) |  | TESTNET: time when the blockclique is ended. |
| thread_count | [uint32](#uint32) |  | Number of threads |
| t0 | [NativeTime](#massa-model-v1-NativeTime) |  | Time between the periods in the same thread. |
| delta_f0 | [uint64](#uint64) |  | Threshold for fitness. |
| operation_validity_periods | [uint64](#uint64) |  | Maximum operation validity period count |
| periods_per_cycle | [uint64](#uint64) |  | cycle duration in periods |
| block_reward | [NativeAmount](#massa-model-v1-NativeAmount) |  | Reward amount for a block creation |
| roll_price | [NativeAmount](#massa-model-v1-NativeAmount) |  | Price of a roll on the network |
| max_block_size | [uint32](#uint32) |  | Max total size of a block |






<a name="massa-model-v1-ConnectedNode"></a>

### ConnectedNode
Connected node


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#string) |  | Node id |
| node_ip | [string](#string) |  | Node ip |
| connection_type | [ConnectionType](#massa-model-v1-ConnectionType) |  | Connection type |






<a name="massa-model-v1-NodeStatus"></a>

### NodeStatus
Node status


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#string) |  | Our node id |
| node_ip | [string](#string) |  | Optional node ip |
| version | [string](#string) |  | Node version |
| current_time | [NativeTime](#massa-model-v1-NativeTime) |  | Now |
| current_cycle | [uint64](#uint64) |  | Current cycle |
| current_cycle_time | [NativeTime](#massa-model-v1-NativeTime) |  | Current cycle starting timestamp |
| next_cycle_time | [NativeTime](#massa-model-v1-NativeTime) |  | Next cycle starting timestamp |
| connected_nodes | [ConnectedNode](#massa-model-v1-ConnectedNode) | repeated | Connected nodes |
| last_executed_final_slot | [Slot](#massa-model-v1-Slot) |  | Last executed final slot |
| last_executed_speculative_slot | [Slot](#massa-model-v1-Slot) |  | Last executed speculative slot |
| final_state_fingerprint | [string](#string) |  | The hash of the XOF final state hash |
| consensus_stats | [ConsensusStats](#massa-model-v1-ConsensusStats) |  | Consensus stats |
| pool_stats | [PoolStats](#massa-model-v1-PoolStats) |  | Pool stats (operation count and endorsement count) |
| network_stats | [NetworkStats](#massa-model-v1-NetworkStats) |  | Network stats |
| execution_stats | [ExecutionStats](#massa-model-v1-ExecutionStats) |  | Execution stats |
| config | [CompactConfig](#massa-model-v1-CompactConfig) |  | Compact configuration |
| chain_id | [uint64](#uint64) |  | Chain id |
| current_mip_version | [uint32](#uint32) |  | Current mip version |






<a name="massa-model-v1-PublicStatus"></a>

### PublicStatus
Public status


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_id | [string](#string) |  | Our node id |
| version | [string](#string) |  | Node version |
| current_time | [NativeTime](#massa-model-v1-NativeTime) |  | Now |
| current_cycle | [uint64](#uint64) |  | Current cycle |
| current_cycle_time | [NativeTime](#massa-model-v1-NativeTime) |  | Current cycle starting timestamp |
| next_cycle_time | [NativeTime](#massa-model-v1-NativeTime) |  | Next cycle starting timestamp |
| last_executed_final_slot | [Slot](#massa-model-v1-Slot) |  | Last executed final slot |
| last_executed_speculative_slot | [Slot](#massa-model-v1-Slot) |  | Last executed speculative slot |
| final_state_fingerprint | [string](#string) |  | The hash of the XOF final state hash |
| config | [CompactConfig](#massa-model-v1-CompactConfig) |  | Compact configuration |
| chain_id | [uint64](#uint64) |  | Chain id |
| minimal_fees | [NativeAmount](#massa-model-v1-NativeAmount) |  | minimal fees |
| current_mip_version | [uint32](#uint32) |  | current mip version |





 


<a name="massa-model-v1-ConnectionType"></a>

### ConnectionType
ConnectionType enum

| Name | Number | Description |
| ---- | ------ | ----------- |
| CONNECTION_TYPE_UNSPECIFIED | 0 | Default enum value |
| CONNECTION_TYPE_INCOMING | 1 | Incoming connection |
| CONNECTION_TYPE_OUTGOING | 2 | Outgoing connection |


 

 

 



<a name="massa_model_v1_operation-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## massa/model/v1/operation.proto



<a name="massa-model-v1-CallSC"></a>

### CallSC
Calls an exported function from a stored smart contract


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| target_address | [string](#string) |  | Target smart contract address |
| target_function | [string](#string) |  | Target function name. No function is called if empty |
| parameter | [bytes](#bytes) |  | Parameter to pass to the target function |
| max_gas | [uint64](#uint64) |  | The maximum amount of gas that the execution of the contract is allowed to cost |
| coins | [NativeAmount](#massa-model-v1-NativeAmount) |  | Extra coins that are spent from the caller&#39;s balance and transferred to the target |






<a name="massa-model-v1-ExecuteSC"></a>

### ExecuteSC
Execute a smart contract


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  | Smart contract bytecode. |
| max_coins | [uint64](#uint64) |  | The maximum of coins that could be spent by the operation sender |
| max_gas | [uint64](#uint64) |  | The maximum amount of gas that the execution of the contract is allowed to cost |
| datastore | [BytesMapFieldEntry](#massa-model-v1-BytesMapFieldEntry) | repeated | A key-value store associating a hash to arbitrary bytes |






<a name="massa-model-v1-OpTypes"></a>

### OpTypes
OpTypes


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| op_types | [OpType](#massa-model-v1-OpType) | repeated | Operations types |






<a name="massa-model-v1-Operation"></a>

### Operation
The operation as sent in the network


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| fee | [NativeAmount](#massa-model-v1-NativeAmount) |  | The fee they have decided for this operation |
| expire_period | [uint64](#uint64) |  | After `expire_period` slot the operation won&#39;t be included in a block |
| op | [OperationType](#massa-model-v1-OperationType) |  | The type specific operation part |






<a name="massa-model-v1-OperationIds"></a>

### OperationIds
OperationIds


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| operation_ids | [string](#string) | repeated | Operations ids |






<a name="massa-model-v1-OperationInfo"></a>

### OperationInfo
Information about an operation with its metadata


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  | The unique ID of the operation. |
| block_ids | [string](#string) | repeated | The IDs of the blocks in which the operation appears |
| thread | [uint32](#uint32) |  | The thread in which the operation can be included |






<a name="massa-model-v1-OperationType"></a>

### OperationType
Type specific operation content


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| transaction | [Transaction](#massa-model-v1-Transaction) |  | Transfer coins from sender to recipient |
| roll_buy | [RollBuy](#massa-model-v1-RollBuy) |  | The sender buys `roll_count` rolls. Roll price is defined in configuration |
| roll_sell | [RollSell](#massa-model-v1-RollSell) |  | The sender sells `roll_count` rolls. Roll price is defined in configuration |
| execute_sc | [ExecuteSC](#massa-model-v1-ExecuteSC) |  | Execute a smart contract |
| call_sc | [CallSC](#massa-model-v1-CallSC) |  | Calls an exported function from a stored smart contract |






<a name="massa-model-v1-OperationTypeRoll"></a>

### OperationTypeRoll



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  |  |
| roll_buy | [RollBuy](#massa-model-v1-RollBuy) |  |  |
| roll_sell | [RollSell](#massa-model-v1-RollSell) |  |  |






<a name="massa-model-v1-OperationWrapper"></a>

### OperationWrapper
A wrapper around an operation with its metadata


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_ids | [string](#string) | repeated | The IDs of the blocks in which the operation appears |
| thread | [uint32](#uint32) |  | The thread in which the operation can be included |
| operation | [SignedOperation](#massa-model-v1-SignedOperation) |  | The operation object itself |






<a name="massa-model-v1-RollBuy"></a>

### RollBuy
The sender buys `roll_count` rolls. Roll price is defined in configuration


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| roll_count | [uint64](#uint64) |  | Roll count |






<a name="massa-model-v1-RollSell"></a>

### RollSell
The sender sells `roll_count` rolls. Roll price is defined in configuration


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| roll_count | [uint64](#uint64) |  | Roll count |






<a name="massa-model-v1-SignedOperation"></a>

### SignedOperation
Signed operation


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| content | [Operation](#massa-model-v1-Operation) |  | Operation |
| signature | [string](#string) |  | A cryptographically generated value using `serialized_data` and a public key. |
| content_creator_pub_key | [string](#string) |  | The public-key component used in the generation of the signature |
| content_creator_address | [string](#string) |  | Derived from the same public key used to generate the signature |
| secure_hash | [string](#string) |  | A secure hash of the non-malleable contents of a deterministic binary representation of the block header |
| serialized_size | [uint64](#uint64) |  | The size of the serialized operation in bytes |






<a name="massa-model-v1-Transaction"></a>

### Transaction
Transfer coins from sender to recipient


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| recipient_address | [string](#string) |  | Recipient address |
| amount | [NativeAmount](#massa-model-v1-NativeAmount) |  | Amount |





 


<a name="massa-model-v1-OpType"></a>

### OpType
Operation type enum

| Name | Number | Description |
| ---- | ------ | ----------- |
| OP_TYPE_UNSPECIFIED | 0 | Default enum value |
| OP_TYPE_TRANSACTION | 1 | Transaction |
| OP_TYPE_ROLL_BUY | 2 | Roll buy |
| OP_TYPE_ROLL_SELL | 3 | Roll sell |
| OP_TYPE_EXECUTE_SC | 4 | Execute smart contract |
| OP_TYPE_CALL_SC | 5 | Call smart contract |


 

 

 



<a name="massa_model_v1_denunciation-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## massa/model/v1/denunciation.proto



<a name="massa-model-v1-BlockHeaderDenunciation"></a>

### BlockHeaderDenunciation



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| public_key | [string](#string) |  | Denunciation public key |
| slot | [Slot](#massa-model-v1-Slot) |  | Denunciation slot |
| hash_1 | [string](#string) |  | Denunciation hash 1 |
| hash_2 | [string](#string) |  | Denunciation hash 2 |
| signature_1 | [string](#string) |  | Denunciation sig 1 |
| signature_2 | [string](#string) |  | Denunciation sig 2 |






<a name="massa-model-v1-Denunciation"></a>

### Denunciation



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_header | [BlockHeaderDenunciation](#massa-model-v1-BlockHeaderDenunciation) |  | Denunciation block header |
| endorsement | [EndorsementDenunciation](#massa-model-v1-EndorsementDenunciation) |  | Denunciation endorsement |
| address | [DenunciationAddress](#massa-model-v1-DenunciationAddress) |  | Denunciation address |






<a name="massa-model-v1-DenunciationAddress"></a>

### DenunciationAddress



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address_denounced | [string](#string) |  | target address |
| slot | [Slot](#massa-model-v1-Slot) |  | slot |
| slashed | [NativeAmount](#massa-model-v1-NativeAmount) |  | amount slashed |






<a name="massa-model-v1-DenunciationBlockHeader"></a>

### DenunciationBlockHeader
Variant for Block header denunciation index


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot | [Slot](#massa-model-v1-Slot) |  | Denunciation slot |






<a name="massa-model-v1-DenunciationEndorsement"></a>

### DenunciationEndorsement
Variant for Endorsement denunciation index


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot | [Slot](#massa-model-v1-Slot) |  | Denounciation slot |
| index | [uint32](#uint32) |  | Denounciation index |






<a name="massa-model-v1-DenunciationIndex"></a>

### DenunciationIndex
Index for Denunciations in collections (e.g. like a HashMap...)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_header | [DenunciationBlockHeader](#massa-model-v1-DenunciationBlockHeader) |  | Denunciation block header |
| endorsement | [DenunciationEndorsement](#massa-model-v1-DenunciationEndorsement) |  | Denunciation endorsement |






<a name="massa-model-v1-EndorsementDenunciation"></a>

### EndorsementDenunciation



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| public_key | [string](#string) |  | Denunciation public key |
| slot | [Slot](#massa-model-v1-Slot) |  | Denunciation slot |
| index | [uint32](#uint32) |  | Denunciation index |
| hash_1 | [string](#string) |  | Denunciation hash 1 |
| hash_2 | [string](#string) |  | Denunciation hash 2 |
| signature_1 | [string](#string) |  | Denunciation sig 1 |
| signature_2 | [string](#string) |  | Denunciation sig 2 |





 

 

 

 



<a name="massa_model_v1_commons-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## massa/model/v1/commons.proto



<a name="massa-model-v1-ArrayOfBytesWrapper"></a>

### ArrayOfBytesWrapper
Array of bytes wrapper


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| items | [bytes](#bytes) | repeated | Repeated bytes |






<a name="massa-model-v1-BytesMapFieldEntry"></a>

### BytesMapFieldEntry
BytesMapFieldEntry


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  | bytes key |
| value | [bytes](#bytes) |  | bytes key |






<a name="massa-model-v1-Empty"></a>

### Empty
Empty






<a name="massa-model-v1-Error"></a>

### Error
Massa error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| code | [int32](#int32) |  | The error code |
| message | [string](#string) |  | A developer-facing error message, which should be in English |






<a name="massa-model-v1-KeyPair"></a>

### KeyPair
KeyPair


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| public_key | [string](#string) |  | Public key |
| secret_key | [string](#string) |  | Secret key |





 


<a name="massa-model-v1-ComparisonResult"></a>

### ComparisonResult
Comparison result

| Name | Number | Description |
| ---- | ------ | ----------- |
| COMPARISON_RESULT_UNSPECIFIED | 0 |  |
| COMPARISON_RESULT_LOWER | 1 | left is lower |
| COMPARISON_RESULT_EQUAL | 2 | left and right are equal |
| COMPARISON_RESULT_GREATER | 3 | left is greater |


 

 

 



<a name="massa_model_v1_amount-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## massa/model/v1/amount.proto



<a name="massa-model-v1-NativeAmount"></a>

### NativeAmount
NativeAmount is represented as a fraction so precision can be adjusted in
the future. value = mantissa / (10^scale)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mantissa | [uint64](#uint64) |  | Mantissa |
| scale | [uint32](#uint32) |  | Scale |





 

 

 

 



<a name="massa_model_v1_execution-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## massa/model/v1/execution.proto



<a name="massa-model-v1-AsyncMessage"></a>

### AsyncMessage
Asynchronous smart contract message


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| emission_slot | [Slot](#massa-model-v1-Slot) |  | Slot at which the message was emitted |
| emission_index | [uint64](#uint64) |  | Index of the emitted message within the `emission_slot`. This is used for disambiguate the emission of multiple messages at the same slot. |
| sender | [string](#string) |  | The address that sent the message |
| destination | [string](#string) |  | The address towards which the message is being sent |
| handler | [string](#string) |  | the handler function name within the destination address&#39; bytecode |
| max_gas | [uint64](#uint64) |  | Maximum gas to use when processing the message |
| fee | [NativeAmount](#massa-model-v1-NativeAmount) |  | Fee paid by the sender when the message is processed. |
| coins | [NativeAmount](#massa-model-v1-NativeAmount) |  | Coins sent from the sender to the target address of the message. Those coins are spent by the sender address when the message is sent, and credited to the destination address when receiving the message. In case of failure or discard, those coins are reimbursed to the sender. |
| validity_start | [Slot](#massa-model-v1-Slot) |  | Slot at which the message starts being valid (bound included in the validity range) |
| validity_end | [Slot](#massa-model-v1-Slot) |  | Slot at which the message stops being valid (bound not included in the validity range) |
| data | [bytes](#bytes) |  | Raw payload data of the message |
| trigger | [AsyncMessageTrigger](#massa-model-v1-AsyncMessageTrigger) |  | Trigger that define whenever a message can be executed |
| can_be_executed | [bool](#bool) |  | Boolean that determine if the message can be executed. For messages without filter this boolean is always true. For messages with filter, this boolean is true if the filter has been matched between `validity_start` and current slot. |






<a name="massa-model-v1-AsyncMessageExecution"></a>

### AsyncMessageExecution



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | [bool](#bool) |  |  |
| sender | [string](#string) |  |  |
| destination | [string](#string) |  |  |
| coins | [NativeAmount](#massa-model-v1-NativeAmount) |  |  |






<a name="massa-model-v1-AsyncMessageTrigger"></a>

### AsyncMessageTrigger
Structure defining a trigger for an asynchronous message


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Filter on the address |
| datastore_key | [google.protobuf.BytesValue](#google-protobuf-BytesValue) |  | Filter on the datastore key (Optional) |






<a name="massa-model-v1-AsyncMessageUpdate"></a>

### AsyncMessageUpdate
Asynchronous smart contract message


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| emission_slot | [SetOrKeepSlot](#massa-model-v1-SetOrKeepSlot) |  | Change the slot at which the message was emitted |
| emission_index | [SetOrKeepUint64](#massa-model-v1-SetOrKeepUint64) |  | Change the index of the emitted message within the `emission_slot`. This is used for disambiguate the emission of multiple messages at the same slot. |
| sender | [SetOrKeepString](#massa-model-v1-SetOrKeepString) |  | Change the address that sent the message |
| destination | [SetOrKeepString](#massa-model-v1-SetOrKeepString) |  | Change the address towards which the message is being sent |
| handler | [SetOrKeepString](#massa-model-v1-SetOrKeepString) |  | Change the handler function name within the destination address&#39; bytecode |
| max_gas | [SetOrKeepUint64](#massa-model-v1-SetOrKeepUint64) |  | Change the maximum gas to use when processing the message |
| fee | [SetOrKeepUint64](#massa-model-v1-SetOrKeepUint64) |  | Change the fee paid by the sender when the message is processed. |
| coins | [SetOrKeepUint64](#massa-model-v1-SetOrKeepUint64) |  | Change the coins sent from the sender to the target address of the message. Those coins are spent by the sender address when the message is sent, and credited to the destination address when receiving the message. In case of failure or discard, those coins are reimbursed to the sender. |
| validity_start | [SetOrKeepSlot](#massa-model-v1-SetOrKeepSlot) |  | Change the slot at which the message starts being valid (bound included in the validity range) |
| validity_end | [SetOrKeepSlot](#massa-model-v1-SetOrKeepSlot) |  | Change the slot at which the message stops being valid (bound not included in the validity range) |
| data | [SetOrKeepBytes](#massa-model-v1-SetOrKeepBytes) |  | Change the raw payload data of the message |
| trigger | [SetOrKeepAsyncMessageTrigger](#massa-model-v1-SetOrKeepAsyncMessageTrigger) |  | Change the trigger that define whenever a message can be executed |
| can_be_executed | [SetOrKeepBool](#massa-model-v1-SetOrKeepBool) |  | Change the boolean that determine if the message can be executed. For messages without filter this boolean is always true. For messages with filter, this boolean is true if the filter has been matched between `validity_start` and current slot. |






<a name="massa-model-v1-AsyncPoolChangeEntry"></a>

### AsyncPoolChangeEntry
AsyncPoolChange Entry


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| async_message_id | [string](#string) |  | Async message id |
| value | [AsyncPoolChangeValue](#massa-model-v1-AsyncPoolChangeValue) |  | AsyncPool message |






<a name="massa-model-v1-AsyncPoolChangeValue"></a>

### AsyncPoolChangeValue
AsyncPoolChangeValue


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [AsyncPoolChangeType](#massa-model-v1-AsyncPoolChangeType) |  | The type of the change |
| created_message | [AsyncMessage](#massa-model-v1-AsyncMessage) |  | Created ledger entry |
| updated_message | [AsyncMessageUpdate](#massa-model-v1-AsyncMessageUpdate) |  | Update ledger entry |






<a name="massa-model-v1-BytecodeExecution"></a>

### BytecodeExecution
Execute a bytecode


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| bytecode | [bytes](#bytes) |  | Byte code |
| operation_datastore | [bytes](#bytes) |  | Serialized datastore (key value store) for `ExecuteSC` Operation (Optional) |






<a name="massa-model-v1-DeferredCallExecution"></a>

### DeferredCallExecution



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| success | [bool](#bool) |  |  |
| sender | [string](#string) |  |  |
| target_address | [string](#string) |  |  |
| target_function | [string](#string) |  |  |
| coins | [NativeAmount](#massa-model-v1-NativeAmount) |  |  |






<a name="massa-model-v1-ExecTransferInfo"></a>

### ExecTransferInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  |  |
| from_address | [google.protobuf.StringValue](#google-protobuf-StringValue) |  |  |
| to_address | [google.protobuf.StringValue](#google-protobuf-StringValue) |  |  |
| value | [TransferValue](#massa-model-v1-TransferValue) |  |  |
| origin | [CoinOrigin](#massa-model-v1-CoinOrigin) |  |  |
| operation_id | [google.protobuf.StringValue](#google-protobuf-StringValue) |  |  |
| async_msg_id | [google.protobuf.StringValue](#google-protobuf-StringValue) |  |  |
| deferred_call_id | [google.protobuf.StringValue](#google-protobuf-StringValue) |  |  |
| denunciation_index | [DenunciationIndex](#massa-model-v1-DenunciationIndex) |  |  |






<a name="massa-model-v1-ExecutedOpsChangeEntry"></a>

### ExecutedOpsChangeEntry
ExecutedOpsChangeEntry


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| operation_id | [string](#string) |  | string |
| value | [ExecutedOpsChangeValue](#massa-model-v1-ExecutedOpsChangeValue) |  | ExecutedOpsChangeValue |






<a name="massa-model-v1-ExecutedOpsChangeValue"></a>

### ExecutedOpsChangeValue
ExecutedOpsChangeValue


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [OperationExecutionStatus](#massa-model-v1-OperationExecutionStatus) |  | The status of the execution of the operation |
| slot | [Slot](#massa-model-v1-Slot) |  | Slot until which the operation remains valid (included) |






<a name="massa-model-v1-ExecutionOutput"></a>

### ExecutionOutput
ExecutionOutput


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot | [Slot](#massa-model-v1-Slot) |  | Slot |
| block_id | [google.protobuf.StringValue](#google-protobuf-StringValue) |  | Block id at that slot (Optional) |
| events | [ScExecutionEvent](#massa-model-v1-ScExecutionEvent) | repeated | Events emitted by the execution step |
| state_changes | [StateChanges](#massa-model-v1-StateChanges) |  | State changes caused by the execution step |






<a name="massa-model-v1-ExecutionStackElement"></a>

### ExecutionStackElement
Structure describing an element of the execution stack.
Every time a function is called from bytecode,
a new `ExecutionStackElement` is pushed at the top of the execution stack
to represent the local execution context of the called function,
instead of the caller&#39;s which should lie just below in the stack.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Called address |
| coins | [NativeAmount](#massa-model-v1-NativeAmount) |  | Coins transferred to the target address during the call |
| owned_addresses | [string](#string) | repeated | List of addresses owned by the current call, and on which the current call has write access. This list should contain `ExecutionStackElement::address` in the sense that an address should have write access to itself. This list should also contain all addresses created previously during the call to allow write access on newly created addresses in order to set them up, but only within the scope of the current stack element. That way, only the current scope and neither its caller not the functions it calls gain this write access, which is important for security. Note that we use a vector instead of a pre-hashed set to ensure order determinism, the performance hit of linear search remains minimal because `owned_addresses` will always contain very few elements. |
| operation_datastore | [BytesMapFieldEntry](#massa-model-v1-BytesMapFieldEntry) | repeated | Datastore (key value store) for `ExecuteSC` Operation (Optional) |






<a name="massa-model-v1-FinalizedExecutionOutput"></a>

### FinalizedExecutionOutput
FinalizedExecutionOutput


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot | [Slot](#massa-model-v1-Slot) |  | Slot |






<a name="massa-model-v1-FunctionCall"></a>

### FunctionCall
Execute a function call


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| target_address | [string](#string) |  | Target address |
| target_function | [string](#string) |  | Target function |
| parameter | [bytes](#bytes) |  | Parameter to pass to the target function |
| coins | [NativeAmount](#massa-model-v1-NativeAmount) |  | Coins transferred to the target address during the call |






<a name="massa-model-v1-LedgerChangeEntry"></a>

### LedgerChangeEntry
LedgerChangeEntry


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address |
| value | [LedgerChangeValue](#massa-model-v1-LedgerChangeValue) |  | Ledger message |






<a name="massa-model-v1-LedgerChangeValue"></a>

### LedgerChangeValue
LedgerChangeValue


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [LedgerChangeType](#massa-model-v1-LedgerChangeType) |  | The type of the change |
| created_entry | [LedgerEntry](#massa-model-v1-LedgerEntry) |  | Created ledger entry |
| updated_entry | [LedgerEntryUpdate](#massa-model-v1-LedgerEntryUpdate) |  | Update ledger entry |






<a name="massa-model-v1-LedgerEntry"></a>

### LedgerEntry
An entry associated to an address in the `FinalLedger`


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| balance | [NativeAmount](#massa-model-v1-NativeAmount) |  | The balance of that entry |
| bytecode | [bytes](#bytes) |  | Executable bytecode |
| datastore | [BytesMapFieldEntry](#massa-model-v1-BytesMapFieldEntry) | repeated | A key-value store associating a hash to arbitrary bytes |






<a name="massa-model-v1-LedgerEntryUpdate"></a>

### LedgerEntryUpdate
Represents an update to one or more fields of a `LedgerEntry`


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| balance | [SetOrKeepBalance](#massa-model-v1-SetOrKeepBalance) |  | Change the balance |
| bytecode | [SetOrKeepBytes](#massa-model-v1-SetOrKeepBytes) |  | Change the executable bytecode |
| datastore | [SetOrDeleteDatastoreEntry](#massa-model-v1-SetOrDeleteDatastoreEntry) | repeated | Change datastore entries |






<a name="massa-model-v1-ReadOnlyExecutionCall"></a>

### ReadOnlyExecutionCall
Read-only execution call


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| max_gas | [uint64](#uint64) |  | Maximum gas to spend in the execution. |
| call_stack | [ExecutionStackElement](#massa-model-v1-ExecutionStackElement) | repeated | Call stack to simulate, older caller first |
| bytecode_call | [BytecodeExecution](#massa-model-v1-BytecodeExecution) |  | Byte code |
| function_call | [FunctionCall](#massa-model-v1-FunctionCall) |  | Function call |
| caller_address | [google.protobuf.StringValue](#google-protobuf-StringValue) |  | Caller&#39;s address, (Optional) if not set, an auto-generated address will be used |
| fee | [NativeAmount](#massa-model-v1-NativeAmount) |  | fee paid by the caller when the call is processed (optional) |






<a name="massa-model-v1-ReadOnlyExecutionOutput"></a>

### ReadOnlyExecutionOutput
Structure describing the output of a read only execution


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| out | [ExecutionOutput](#massa-model-v1-ExecutionOutput) |  | Output of a single execution |
| used_gas | [uint64](#uint64) |  | Gas cost for this execution |
| call_result | [bytes](#bytes) |  | Returned value from the module call |






<a name="massa-model-v1-ScExecutionEvent"></a>

### ScExecutionEvent
ScExecutionEvent


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| context | [ScExecutionEventContext](#massa-model-v1-ScExecutionEventContext) |  | Sc execution context |
| data | [bytes](#bytes) |  | Generated data of the event |






<a name="massa-model-v1-ScExecutionEventContext"></a>

### ScExecutionEventContext
ScExecutionEvent context


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| origin_slot | [Slot](#massa-model-v1-Slot) |  | When was it generated |
| block_id | [google.protobuf.StringValue](#google-protobuf-StringValue) |  | Block id if there was a block at that slot (Optional) |
| index_in_slot | [uint64](#uint64) |  | Index of the event in the slot |
| call_stack | [string](#string) | repeated | Call stack addresses. most recent at the end |
| origin_operation_id | [google.protobuf.StringValue](#google-protobuf-StringValue) |  | Origin operation id (Optional) |
| is_failure | [bool](#bool) |  | If a failure occurred |
| status | [ScExecutionEventStatus](#massa-model-v1-ScExecutionEventStatus) |  | Status |






<a name="massa-model-v1-ScExecutionEventsStatus"></a>

### ScExecutionEventsStatus
ScExecutionEventsStatus


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [ScExecutionEventStatus](#massa-model-v1-ScExecutionEventStatus) | repeated | Status |






<a name="massa-model-v1-SetOrDeleteDatastoreEntry"></a>

### SetOrDeleteDatastoreEntry
Set or Delete DatastoreEntry


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| set | [BytesMapFieldEntry](#massa-model-v1-BytesMapFieldEntry) |  | Executable bytecode (Optional) |
| delete | [Empty](#massa-model-v1-Empty) |  | Delete the existing bytecode |






<a name="massa-model-v1-SetOrKeepAsyncMessageTrigger"></a>

### SetOrKeepAsyncMessageTrigger
Set or Keep AsyncMessageTrigger


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| set | [AsyncMessageTrigger](#massa-model-v1-AsyncMessageTrigger) |  | The value of that entry (Optional) |
| keep | [Empty](#massa-model-v1-Empty) |  | Keep the existing value |






<a name="massa-model-v1-SetOrKeepBalance"></a>

### SetOrKeepBalance
Set or Keep Balance


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| set | [NativeAmount](#massa-model-v1-NativeAmount) |  | The value of that entry (Optional) |
| keep | [Empty](#massa-model-v1-Empty) |  | Keep the existing value |






<a name="massa-model-v1-SetOrKeepBool"></a>

### SetOrKeepBool
Set or Keep Bool


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| set | [google.protobuf.BoolValue](#google-protobuf-BoolValue) |  | The value of that entry (Optional) |
| keep | [Empty](#massa-model-v1-Empty) |  | Keep the existing value |






<a name="massa-model-v1-SetOrKeepBytes"></a>

### SetOrKeepBytes
Set or Keep Bytes


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| set | [google.protobuf.BytesValue](#google-protobuf-BytesValue) |  | The value of that entry (Optional) |
| keep | [Empty](#massa-model-v1-Empty) |  | Keep the existing value |






<a name="massa-model-v1-SetOrKeepSlot"></a>

### SetOrKeepSlot
Set or Keep Slot


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| set | [Slot](#massa-model-v1-Slot) |  | The value of that entry (Optional) |
| keep | [Empty](#massa-model-v1-Empty) |  | Keep the existing value |






<a name="massa-model-v1-SetOrKeepString"></a>

### SetOrKeepString
Set or Keep String


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| set | [google.protobuf.StringValue](#google-protobuf-StringValue) |  | The value of that entry (Optional) |
| keep | [Empty](#massa-model-v1-Empty) |  | Keep the existing value |






<a name="massa-model-v1-SetOrKeepUint64"></a>

### SetOrKeepUint64
Set or Keep Uint64


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| set | [google.protobuf.UInt64Value](#google-protobuf-UInt64Value) |  | The value of that entry (Optional) |
| keep | [Empty](#massa-model-v1-Empty) |  | Keep the existing value |






<a name="massa-model-v1-SlotExecutionOutput"></a>

### SlotExecutionOutput
SlotExecutionOutput


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [ExecutionOutputStatus](#massa-model-v1-ExecutionOutputStatus) |  | Status |
| execution_output | [ExecutionOutput](#massa-model-v1-ExecutionOutput) |  | Executed slot output |






<a name="massa-model-v1-StateChanges"></a>

### StateChanges
StateChanges


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ledger_changes | [LedgerChangeEntry](#massa-model-v1-LedgerChangeEntry) | repeated | Ledger changes |
| async_pool_changes | [AsyncPoolChangeEntry](#massa-model-v1-AsyncPoolChangeEntry) | repeated | Asynchronous pool changes |
| executed_ops_changes | [ExecutedOpsChangeEntry](#massa-model-v1-ExecutedOpsChangeEntry) | repeated | Executed operations changes |
| executed_denunciations_changes | [DenunciationIndex](#massa-model-v1-DenunciationIndex) | repeated | Executed denunciations changes |
| execution_trail_hash_change | [SetOrKeepString](#massa-model-v1-SetOrKeepString) |  | Execution trail hash change |






<a name="massa-model-v1-TargetAmount"></a>

### TargetAmount



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| amount | [NativeAmount](#massa-model-v1-NativeAmount) |  |  |
| address | [string](#string) |  |  |






<a name="massa-model-v1-TransferValue"></a>

### TransferValue



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| rolls | [uint64](#uint64) |  |  |
| coins | [NativeAmount](#massa-model-v1-NativeAmount) |  |  |
| deferred_credits | [NativeAmount](#massa-model-v1-NativeAmount) |  |  |





 


<a name="massa-model-v1-AsyncPoolChangeType"></a>

### AsyncPoolChangeType
AsyncPoolChangeType type enum

| Name | Number | Description |
| ---- | ------ | ----------- |
| ASYNC_POOL_CHANGE_TYPE_UNSPECIFIED | 0 | Default enum value |
| ASYNC_POOL_CHANGE_TYPE_SET | 1 | Set type |
| ASYNC_POOL_CHANGE_TYPE_UPDATE | 2 | Activate only type |
| ASYNC_POOL_CHANGE_TYPE_DELETE | 3 | Delete only type |



<a name="massa-model-v1-CoinOrigin"></a>

### CoinOrigin


| Name | Number | Description |
| ---- | ------ | ----------- |
| COIN_ORIGIN_UNSPECIFIED | 0 |  |
| COIN_ORIGIN_BLOCK_REWARD | 1 |  |
| COIN_ORIGIN_DEFERRED_CALL_FAIL | 2 |  |
| COIN_ORIGIN_DEFERRED_CALL_CANCEL | 3 |  |
| COIN_ORIGIN_DEFERRED_CALL_COINS | 4 |  |
| COIN_ORIGIN_DEFERRED_CALL_REGISTER | 5 |  |
| COIN_ORIGIN_DEFERRED_CALL_STORAGE_REFUND | 6 |  |
| COIN_ORIGIN_ENDORSEMENT_REWARD | 7 |  |
| COIN_ORIGIN_ENDORSED_REWARD | 8 |  |
| COIN_ORIGIN_SLASH | 9 |  |
| COIN_ORIGIN_OP_ROLL_BUY | 10 |  |
| COIN_ORIGIN_OP_ROLL_SELL | 11 |  |
| COIN_ORIGIN_OP_CALLSC_COINS | 12 |  |
| COIN_ORIGIN_READ_ONLY_FN_CALL_FEES | 13 |  |
| COIN_ORIGIN_READ_ONLY_FN_CALL_COINS | 14 |  |
| COIN_ORIGIN_READ_ONLY_BYTECODE_EXEC_FEES | 15 |  |
| COIN_ORIGIN_SET_BYTECODE_STORAGE | 16 |  |
| COIN_ORIGIN_ABI_CALL_COINS | 17 |  |
| COIN_ORIGIN_ABI_TRANSFER_COINS | 18 |  |
| COIN_ORIGIN_ABI_TRANSFER_FOR_COINS | 19 |  |
| COIN_ORIGIN_ABI_SEND_MSG_COINS | 20 |  |
| COIN_ORIGIN_ABI_SEND_MSG_FEES | 21 |  |
| COIN_ORIGIN_OP_ROLL_SELL_DEFERRED_MAS | 22 |  |
| COIN_ORIGIN_OP_EXECUTESC_FEES | 23 |  |
| COIN_ORIGIN_OP_TRANSACTION_COINS | 24 |  |
| COIN_ORIGIN_OP_TRANSACTION_FEES | 25 |  |
| COIN_ORIGIN_ASYNC_MSG_COINS | 26 |  |
| COIN_ORIGIN_ASYNC_MSG_CANCEL | 27 |  |
| COIN_ORIGIN_CREATE_SC_STORAGE | 28 |  |
| COIN_ORIGIN_DATASTORE_STORAGE | 29 |  |
| COIN_ORIGIN_DEFERRED_CREDIT | 30 |  |



<a name="massa-model-v1-ExecutionOutputStatus"></a>

### ExecutionOutputStatus
ExecutionOutputStatus type enum

| Name | Number | Description |
| ---- | ------ | ----------- |
| EXECUTION_OUTPUT_STATUS_UNSPECIFIED | 0 | Default enum value |
| EXECUTION_OUTPUT_STATUS_CANDIDATE | 1 | Candidate status |
| EXECUTION_OUTPUT_STATUS_FINAL | 2 | Final status |
| EXECUTION_OUTPUT_STATUS_UNKNOWN | 3 | Unknown status |



<a name="massa-model-v1-LedgerChangeType"></a>

### LedgerChangeType
LedgerChangeType type enum

| Name | Number | Description |
| ---- | ------ | ----------- |
| LEDGER_CHANGE_TYPE_UNSPECIFIED | 0 | Default enum value |
| LEDGER_CHANGE_TYPE_SET | 1 | Set type |
| LEDGER_CHANGE_TYPE_UPDATE | 2 | Update type |
| LEDGER_CHANGE_TYPE_DELETE | 3 | Delete type |



<a name="massa-model-v1-OperationExecutionStatus"></a>

### OperationExecutionStatus
OperationExecutionStatus type enum

| Name | Number | Description |
| ---- | ------ | ----------- |
| OPERATION_EXECUTION_STATUS_UNSPECIFIED | 0 | Default enum value |
| OPERATION_EXECUTION_STATUS_SUCCESS | 1 | Success status |
| OPERATION_EXECUTION_STATUS_FAILED | 2 | Failed only status |



<a name="massa-model-v1-ScExecutionEventStatus"></a>

### ScExecutionEventStatus
ScExecutionEventStatus type enum

| Name | Number | Description |
| ---- | ------ | ----------- |
| SC_EXECUTION_EVENT_STATUS_UNSPECIFIED | 0 | Default enum value |
| SC_EXECUTION_EVENT_STATUS_FINAL | 1 | Final status |
| SC_EXECUTION_EVENT_STATUS_READ_ONLY | 2 | Read only status |
| SC_EXECUTION_EVENT_STATUS_CANDIDATE | 3 | Candidate status |


 

 

 



<a name="massa_model_v1_slot-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## massa/model/v1/slot.proto



<a name="massa-model-v1-IndexedSlot"></a>

### IndexedSlot
When an address is drawn to create an endorsement it is selected for a specific index


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot | [Slot](#massa-model-v1-Slot) |  | Slot |
| index | [uint64](#uint64) |  | Endorsement index in the slot |






<a name="massa-model-v1-Slot"></a>

### Slot
A point in time where a block is expected


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| period | [uint64](#uint64) |  | Period |
| thread | [uint32](#uint32) |  | Thread |






<a name="massa-model-v1-SlotRange"></a>

### SlotRange
SlotRange


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| start_slot | [Slot](#massa-model-v1-Slot) |  | Start lot (Optional) |
| end_slot | [Slot](#massa-model-v1-Slot) |  | End slot (Optional) |






<a name="massa-model-v1-Slots"></a>

### Slots
Slots


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slots | [Slot](#massa-model-v1-Slot) | repeated | Slots |





 

 

 

 



<a name="massa_model_v1_address-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## massa/model/v1/address.proto



<a name="massa-model-v1-Addresses"></a>

### Addresses
Addresses holds addresses


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| addresses | [string](#string) | repeated | Addresses |






<a name="massa-model-v1-NativeAddress"></a>

### NativeAddress
Massa NativeAddress


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| category | [AddressCategory](#massa-model-v1-AddressCategory) |  | Address category |
| version | [uint64](#uint64) |  | Address version |
| content | [bytes](#bytes) |  | Address content |





 


<a name="massa-model-v1-AddressCategory"></a>

### AddressCategory
Address category discriminant

| Name | Number | Description |
| ---- | ------ | ----------- |
| ADDRESS_CATEGORY_UNSPECIFIED | 0 | Unspecified address category |
| ADDRESS_CATEGORY_USER_ADDRESS | 1 | User address |
| ADDRESS_CATEGORY_SC_ADDRESS | 2 | Smart contract address |


 

 

 



<a name="massa_model_v1_block-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## massa/model/v1/block.proto



<a name="massa-model-v1-Block"></a>

### Block
Block


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [SignedBlockHeader](#massa-model-v1-SignedBlockHeader) |  | Signed header |
| operations | [string](#string) | repeated | Operations ids |






<a name="massa-model-v1-BlockHeader"></a>

### BlockHeader
Block header


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| current_version | [uint32](#uint32) |  | Current network version |
| announced_version | [google.protobuf.UInt32Value](#google-protobuf-UInt32Value) |  | Announced network version(Optional) |
| slot | [Slot](#massa-model-v1-Slot) |  | Slot |
| parents | [string](#string) | repeated | parents |
| operations_hash | [string](#string) |  | All operations hash |
| endorsements | [SignedEndorsement](#massa-model-v1-SignedEndorsement) | repeated | Signed endorsements |
| denunciations | [Denunciation](#massa-model-v1-Denunciation) | repeated | Denunciations |






<a name="massa-model-v1-BlockIds"></a>

### BlockIds
BlockIds holds block ids


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_ids | [string](#string) | repeated | Block ids |






<a name="massa-model-v1-BlockInfo"></a>

### BlockInfo
Informations about a block with its metadata


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_id | [string](#string) |  | The unique ID of the block. |
| status | [BlockStatus](#massa-model-v1-BlockStatus) |  | The execution status of the block |






<a name="massa-model-v1-BlockParent"></a>

### BlockParent
Block parent tuple


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_id | [string](#string) |  | Block id |
| period | [uint64](#uint64) |  | Period |






<a name="massa-model-v1-BlockWrapper"></a>

### BlockWrapper
A wrapper around a block with its metadata


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [BlockStatus](#massa-model-v1-BlockStatus) |  | The execution status of the block |
| block | [Block](#massa-model-v1-Block) |  | The block object itself |






<a name="massa-model-v1-FilledBlock"></a>

### FilledBlock
Filled block


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [SignedBlockHeader](#massa-model-v1-SignedBlockHeader) |  | Signed header |
| operations | [FilledOperationEntry](#massa-model-v1-FilledOperationEntry) | repeated | Operations |






<a name="massa-model-v1-FilledOperationEntry"></a>

### FilledOperationEntry
Filled Operation Tuple


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| operation_id | [string](#string) |  | Operation id |
| operation | [SignedOperation](#massa-model-v1-SignedOperation) |  | Signed operation |






<a name="massa-model-v1-SignedBlock"></a>

### SignedBlock
Signed block


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| content | [Block](#massa-model-v1-Block) |  | Block |
| signature | [string](#string) |  | A cryptographically generated value using `serialized_data` and a public key. |
| content_creator_pub_key | [string](#string) |  | The public-key component used in the generation of the signature |
| content_creator_address | [string](#string) |  | Derived from the same public key used to generate the signature |
| secure_hash | [string](#string) |  | A secure hash of the non-malleable contents of a deterministic binary representation of the block header |
| serialized_size | [uint64](#uint64) |  | The size of the serialized block in bytes |






<a name="massa-model-v1-SignedBlockHeader"></a>

### SignedBlockHeader
Signed block header


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| content | [BlockHeader](#massa-model-v1-BlockHeader) |  | BlockHeader |
| signature | [string](#string) |  | A cryptographically generated value using `serialized_data` and a public key. |
| content_creator_pub_key | [string](#string) |  | The public-key component used in the generation of the signature |
| content_creator_address | [string](#string) |  | Derived from the same public key used to generate the signature |
| secure_hash | [string](#string) |  | A secure hash of the non-malleable contents of a deterministic binary representation of the block header |
| serialized_size | [uint64](#uint64) |  | The size of the serialized block header in bytes |





 


<a name="massa-model-v1-BlockStatus"></a>

### BlockStatus
Possible statuses for a block

| Name | Number | Description |
| ---- | ------ | ----------- |
| BLOCK_STATUS_UNSPECIFIED | 0 | Default enum value |
| BLOCK_STATUS_NON_FINAL_BLOCKCLIQUE | 1 | The block is in the greatest clique (and not final) |
| BLOCK_STATUS_FINAL | 2 | The block is final |
| BLOCK_STATUS_NON_FINAL_ALTERNATE_CLIQUE | 3 | The block is candidate (active any clique but not final) |
| BLOCK_STATUS_DISCARDED | 4 | The block is discarded |


 

 

 



<a name="massa_model_v1_endorsement-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## massa/model/v1/endorsement.proto



<a name="massa-model-v1-Endorsement"></a>

### Endorsement
An endorsement, as sent in the network


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot | [Slot](#massa-model-v1-Slot) |  | Slot in which the endorsement can be included |
| index | [uint32](#uint32) |  | Endorsement index inside the including block |
| endorsed_block | [string](#string) |  | Hash of endorsed block This is the parent in thread `self.slot.thread` of the block in which the endorsement is included |






<a name="massa-model-v1-EndorsementIds"></a>

### EndorsementIds
EndorsementIds holds endorsements ids


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| endorsement_ids | [string](#string) | repeated | Endorsements ids |






<a name="massa-model-v1-EndorsementInfo"></a>

### EndorsementInfo
Informations about an endorsement with its metadata


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| endorsement_id | [string](#string) |  | The endorsement id |
| in_pool | [bool](#bool) |  | Whether the endorsement is still in pool |
| in_blocks | [string](#string) | repeated | The endorsement appears in `in_blocks` If it appears in multiple blocks, these blocks are in different cliques |
| is_final | [bool](#bool) |  | Whether the the endorsement is final (for example in a final block) |






<a name="massa-model-v1-EndorsementWrapper"></a>

### EndorsementWrapper
A wrapper around an endorsement with its metadata


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| in_pool | [bool](#bool) |  | Whether the endorsement is still in pool |
| in_blocks | [string](#string) | repeated | The endorsement appears in `in_blocks` If it appears in multiple blocks, these blocks are in different cliques |
| is_final | [bool](#bool) |  | Whether the the endorsement is final (for example in a final block) |
| endorsement | [SignedEndorsement](#massa-model-v1-SignedEndorsement) |  | The endorsement itself |






<a name="massa-model-v1-SignedEndorsement"></a>

### SignedEndorsement
Signed endorsement


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| content | [Endorsement](#massa-model-v1-Endorsement) |  | Endorsement |
| signature | [string](#string) |  | A cryptographically generated value using `serialized_data` and a public key. |
| content_creator_pub_key | [string](#string) |  | The public-key component used in the generation of the signature |
| content_creator_address | [string](#string) |  | Derived from the same public key used to generate the signature |
| secure_hash | [string](#string) |  | A secure hash of the non-malleable contents of a deterministic binary representation of the block header |
| serialized_size | [uint64](#uint64) |  | The size of the serialized endorsement in bytes |





 

 

 

 



<a name="massa_model_v1_time-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## massa/model/v1/time.proto



<a name="massa-model-v1-NativeTime"></a>

### NativeTime
NativeTime represents a native duration or unix timestamp


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| milliseconds | [uint64](#uint64) |  | Milliseconds |





 

 

 

 



## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |

