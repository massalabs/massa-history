# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [massa/api/v1/private.proto](#massa_api_v1_private-proto)
    - [AddStakingSecretKeysRequest](#massa-api-v1-AddStakingSecretKeysRequest)
    - [AddStakingSecretKeysResponse](#massa-api-v1-AddStakingSecretKeysResponse)
    - [AddToBootstrapBlacklistRequest](#massa-api-v1-AddToBootstrapBlacklistRequest)
    - [AddToBootstrapBlacklistResponse](#massa-api-v1-AddToBootstrapBlacklistResponse)
    - [AddToBootstrapWhitelistRequest](#massa-api-v1-AddToBootstrapWhitelistRequest)
    - [AddToBootstrapWhitelistResponse](#massa-api-v1-AddToBootstrapWhitelistResponse)
    - [AddToPeersWhitelistRequest](#massa-api-v1-AddToPeersWhitelistRequest)
    - [AddToPeersWhitelistResponse](#massa-api-v1-AddToPeersWhitelistResponse)
    - [AllowEveryoneToBootstrapRequest](#massa-api-v1-AllowEveryoneToBootstrapRequest)
    - [AllowEveryoneToBootstrapResponse](#massa-api-v1-AllowEveryoneToBootstrapResponse)
    - [BanNodesByIdsRequest](#massa-api-v1-BanNodesByIdsRequest)
    - [BanNodesByIdsResponse](#massa-api-v1-BanNodesByIdsResponse)
    - [BanNodesByIpsRequest](#massa-api-v1-BanNodesByIpsRequest)
    - [BanNodesByIpsResponse](#massa-api-v1-BanNodesByIpsResponse)
    - [GetBootstrapBlacklistRequest](#massa-api-v1-GetBootstrapBlacklistRequest)
    - [GetBootstrapBlacklistResponse](#massa-api-v1-GetBootstrapBlacklistResponse)
    - [GetBootstrapWhitelistRequest](#massa-api-v1-GetBootstrapWhitelistRequest)
    - [GetBootstrapWhitelistResponse](#massa-api-v1-GetBootstrapWhitelistResponse)
    - [GetMipStatusRequest](#massa-api-v1-GetMipStatusRequest)
    - [GetMipStatusResponse](#massa-api-v1-GetMipStatusResponse)
    - [GetNodeStatusRequest](#massa-api-v1-GetNodeStatusRequest)
    - [GetNodeStatusResponse](#massa-api-v1-GetNodeStatusResponse)
    - [GetPeersWhitelistRequest](#massa-api-v1-GetPeersWhitelistRequest)
    - [GetPeersWhitelistResponse](#massa-api-v1-GetPeersWhitelistResponse)
    - [RemoveFromBootstrapBlacklistRequest](#massa-api-v1-RemoveFromBootstrapBlacklistRequest)
    - [RemoveFromBootstrapBlacklistResponse](#massa-api-v1-RemoveFromBootstrapBlacklistResponse)
    - [RemoveFromBootstrapWhitelistRequest](#massa-api-v1-RemoveFromBootstrapWhitelistRequest)
    - [RemoveFromBootstrapWhitelistResponse](#massa-api-v1-RemoveFromBootstrapWhitelistResponse)
    - [RemoveFromPeersWhitelistRequest](#massa-api-v1-RemoveFromPeersWhitelistRequest)
    - [RemoveFromPeersWhitelistResponse](#massa-api-v1-RemoveFromPeersWhitelistResponse)
    - [RemoveStakingAddressesRequest](#massa-api-v1-RemoveStakingAddressesRequest)
    - [RemoveStakingAddressesResponse](#massa-api-v1-RemoveStakingAddressesResponse)
    - [ShutdownGracefullyRequest](#massa-api-v1-ShutdownGracefullyRequest)
    - [ShutdownGracefullyResponse](#massa-api-v1-ShutdownGracefullyResponse)
    - [SignMessagesRequest](#massa-api-v1-SignMessagesRequest)
    - [SignMessagesResponse](#massa-api-v1-SignMessagesResponse)
    - [UnbanNodesByIdsRequest](#massa-api-v1-UnbanNodesByIdsRequest)
    - [UnbanNodesByIdsResponse](#massa-api-v1-UnbanNodesByIdsResponse)
    - [UnbanNodesByIpsRequest](#massa-api-v1-UnbanNodesByIpsRequest)
    - [UnbanNodesByIpsResponse](#massa-api-v1-UnbanNodesByIpsResponse)
  
    - [PrivateService](#massa-api-v1-PrivateService)
  
- [massa/api/v1/public.proto](#massa_api_v1_public-proto)
    - [ABICallStack](#massa-api-v1-ABICallStack)
    - [ABICallStackElement](#massa-api-v1-ABICallStackElement)
    - [ABICallStackElementCall](#massa-api-v1-ABICallStackElementCall)
    - [ABICallStackElementParent](#massa-api-v1-ABICallStackElementParent)
    - [ASCABICallStack](#massa-api-v1-ASCABICallStack)
    - [AddressBalanceCandidate](#massa-api-v1-AddressBalanceCandidate)
    - [AddressBalanceFinal](#massa-api-v1-AddressBalanceFinal)
    - [AddressBytecodeCandidate](#massa-api-v1-AddressBytecodeCandidate)
    - [AddressBytecodeFinal](#massa-api-v1-AddressBytecodeFinal)
    - [AddressDatastoreKeysCandidate](#massa-api-v1-AddressDatastoreKeysCandidate)
    - [AddressDatastoreKeysFinal](#massa-api-v1-AddressDatastoreKeysFinal)
    - [AddressDatastoreValueCandidate](#massa-api-v1-AddressDatastoreValueCandidate)
    - [AddressDatastoreValueFinal](#massa-api-v1-AddressDatastoreValueFinal)
    - [AddressDeferredCreditsCandidate](#massa-api-v1-AddressDeferredCreditsCandidate)
    - [AddressDeferredCreditsFinal](#massa-api-v1-AddressDeferredCreditsFinal)
    - [AddressExistsCandidate](#massa-api-v1-AddressExistsCandidate)
    - [AddressExistsFinal](#massa-api-v1-AddressExistsFinal)
    - [AddressRollsCandidate](#massa-api-v1-AddressRollsCandidate)
    - [AddressRollsFinal](#massa-api-v1-AddressRollsFinal)
    - [AsyncPoolChangesFilter](#massa-api-v1-AsyncPoolChangesFilter)
    - [CycleInfos](#massa-api-v1-CycleInfos)
    - [DeferredCallABICallStack](#massa-api-v1-DeferredCallABICallStack)
    - [DeferredCallInfo](#massa-api-v1-DeferredCallInfo)
    - [DeferredCallInfoEntry](#massa-api-v1-DeferredCallInfoEntry)
    - [DeferredCallInfoResponse](#massa-api-v1-DeferredCallInfoResponse)
    - [DeferredCallQuote](#massa-api-v1-DeferredCallQuote)
    - [DeferredCallQuoteResponse](#massa-api-v1-DeferredCallQuoteResponse)
    - [DeferredCallsBySlot](#massa-api-v1-DeferredCallsBySlot)
    - [DeferredCallsBySlotResponse](#massa-api-v1-DeferredCallsBySlotResponse)
    - [DeferredCreditsEntry](#massa-api-v1-DeferredCreditsEntry)
    - [DeferredCreditsEntryWrapper](#massa-api-v1-DeferredCreditsEntryWrapper)
    - [DenunciationExecutionStatusCandidate](#massa-api-v1-DenunciationExecutionStatusCandidate)
    - [DenunciationExecutionStatusFinal](#massa-api-v1-DenunciationExecutionStatusFinal)
    - [Events](#massa-api-v1-Events)
    - [ExecuteReadOnlyCallRequest](#massa-api-v1-ExecuteReadOnlyCallRequest)
    - [ExecuteReadOnlyCallResponse](#massa-api-v1-ExecuteReadOnlyCallResponse)
    - [ExecutedDenounciationFilter](#massa-api-v1-ExecutedDenounciationFilter)
    - [ExecutedOpsChangesFilter](#massa-api-v1-ExecutedOpsChangesFilter)
    - [ExecutionEventFilter](#massa-api-v1-ExecutionEventFilter)
    - [ExecutionQueryCycleInfos](#massa-api-v1-ExecutionQueryCycleInfos)
    - [ExecutionQueryRequestItem](#massa-api-v1-ExecutionQueryRequestItem)
    - [ExecutionQueryResponse](#massa-api-v1-ExecutionQueryResponse)
    - [ExecutionQueryResponseItem](#massa-api-v1-ExecutionQueryResponseItem)
    - [ExecutionQueryStakerInfo](#massa-api-v1-ExecutionQueryStakerInfo)
    - [ExecutionQueryStakerInfoEntry](#massa-api-v1-ExecutionQueryStakerInfoEntry)
    - [ExecutionQueryStakerInfoProductionStats](#massa-api-v1-ExecutionQueryStakerInfoProductionStats)
    - [ExecutionQueryStakerInfoProductionStatsEntry](#massa-api-v1-ExecutionQueryStakerInfoProductionStatsEntry)
    - [GetBlocksRequest](#massa-api-v1-GetBlocksRequest)
    - [GetBlocksResponse](#massa-api-v1-GetBlocksResponse)
    - [GetDatastoreEntriesRequest](#massa-api-v1-GetDatastoreEntriesRequest)
    - [GetDatastoreEntriesResponse](#massa-api-v1-GetDatastoreEntriesResponse)
    - [GetDatastoreEntryFilter](#massa-api-v1-GetDatastoreEntryFilter)
    - [GetEndorsementsRequest](#massa-api-v1-GetEndorsementsRequest)
    - [GetEndorsementsResponse](#massa-api-v1-GetEndorsementsResponse)
    - [GetNextBlockBestParentsRequest](#massa-api-v1-GetNextBlockBestParentsRequest)
    - [GetNextBlockBestParentsResponse](#massa-api-v1-GetNextBlockBestParentsResponse)
    - [GetOperationABICallStacksRequest](#massa-api-v1-GetOperationABICallStacksRequest)
    - [GetOperationABICallStacksResponse](#massa-api-v1-GetOperationABICallStacksResponse)
    - [GetOperationsRequest](#massa-api-v1-GetOperationsRequest)
    - [GetOperationsResponse](#massa-api-v1-GetOperationsResponse)
    - [GetScExecutionEventsRequest](#massa-api-v1-GetScExecutionEventsRequest)
    - [GetScExecutionEventsResponse](#massa-api-v1-GetScExecutionEventsResponse)
    - [GetSelectorDrawsRequest](#massa-api-v1-GetSelectorDrawsRequest)
    - [GetSelectorDrawsResponse](#massa-api-v1-GetSelectorDrawsResponse)
    - [GetSlotABICallStacksRequest](#massa-api-v1-GetSlotABICallStacksRequest)
    - [GetSlotABICallStacksResponse](#massa-api-v1-GetSlotABICallStacksResponse)
    - [GetSlotTransfersRequest](#massa-api-v1-GetSlotTransfersRequest)
    - [GetSlotTransfersResponse](#massa-api-v1-GetSlotTransfersResponse)
    - [GetStakersRequest](#massa-api-v1-GetStakersRequest)
    - [GetStakersResponse](#massa-api-v1-GetStakersResponse)
    - [GetStatusRequest](#massa-api-v1-GetStatusRequest)
    - [GetStatusResponse](#massa-api-v1-GetStatusResponse)
    - [GetTransactionsThroughputRequest](#massa-api-v1-GetTransactionsThroughputRequest)
    - [GetTransactionsThroughputResponse](#massa-api-v1-GetTransactionsThroughputResponse)
    - [LedgerChangesFilter](#massa-api-v1-LedgerChangesFilter)
    - [NewBlocksFilter](#massa-api-v1-NewBlocksFilter)
    - [NewBlocksRequest](#massa-api-v1-NewBlocksRequest)
    - [NewBlocksResponse](#massa-api-v1-NewBlocksResponse)
    - [NewBlocksServerRequest](#massa-api-v1-NewBlocksServerRequest)
    - [NewBlocksServerResponse](#massa-api-v1-NewBlocksServerResponse)
    - [NewEndorsementsFilter](#massa-api-v1-NewEndorsementsFilter)
    - [NewEndorsementsRequest](#massa-api-v1-NewEndorsementsRequest)
    - [NewEndorsementsResponse](#massa-api-v1-NewEndorsementsResponse)
    - [NewEndorsementsServerRequest](#massa-api-v1-NewEndorsementsServerRequest)
    - [NewEndorsementsServerResponse](#massa-api-v1-NewEndorsementsServerResponse)
    - [NewFilledBlocksFilter](#massa-api-v1-NewFilledBlocksFilter)
    - [NewFilledBlocksRequest](#massa-api-v1-NewFilledBlocksRequest)
    - [NewFilledBlocksResponse](#massa-api-v1-NewFilledBlocksResponse)
    - [NewFilledBlocksServerRequest](#massa-api-v1-NewFilledBlocksServerRequest)
    - [NewFilledBlocksServerResponse](#massa-api-v1-NewFilledBlocksServerResponse)
    - [NewOperationsFilter](#massa-api-v1-NewOperationsFilter)
    - [NewOperationsRequest](#massa-api-v1-NewOperationsRequest)
    - [NewOperationsResponse](#massa-api-v1-NewOperationsResponse)
    - [NewOperationsServerRequest](#massa-api-v1-NewOperationsServerRequest)
    - [NewOperationsServerResponse](#massa-api-v1-NewOperationsServerResponse)
    - [NewSlotABICallStacksRequest](#massa-api-v1-NewSlotABICallStacksRequest)
    - [NewSlotABICallStacksResponse](#massa-api-v1-NewSlotABICallStacksResponse)
    - [NewSlotExecutionOutputsFilter](#massa-api-v1-NewSlotExecutionOutputsFilter)
    - [NewSlotExecutionOutputsRequest](#massa-api-v1-NewSlotExecutionOutputsRequest)
    - [NewSlotExecutionOutputsResponse](#massa-api-v1-NewSlotExecutionOutputsResponse)
    - [NewSlotExecutionOutputsServerRequest](#massa-api-v1-NewSlotExecutionOutputsServerRequest)
    - [NewSlotExecutionOutputsServerResponse](#massa-api-v1-NewSlotExecutionOutputsServerResponse)
    - [NewSlotTransfersRequest](#massa-api-v1-NewSlotTransfersRequest)
    - [NewSlotTransfersResponse](#massa-api-v1-NewSlotTransfersResponse)
    - [NewTransfersInfoServerRequest](#massa-api-v1-NewTransfersInfoServerRequest)
    - [NewTransfersInfoServerResponse](#massa-api-v1-NewTransfersInfoServerResponse)
    - [OpExecutionStatusCandidate](#massa-api-v1-OpExecutionStatusCandidate)
    - [OpExecutionStatusFinal](#massa-api-v1-OpExecutionStatusFinal)
    - [OperationABICallStack](#massa-api-v1-OperationABICallStack)
    - [PosChangesFilter](#massa-api-v1-PosChangesFilter)
    - [QueryStateRequest](#massa-api-v1-QueryStateRequest)
    - [QueryStateResponse](#massa-api-v1-QueryStateResponse)
    - [ScExecutionEventsFilter](#massa-api-v1-ScExecutionEventsFilter)
    - [ScOutputEventsWrapper](#massa-api-v1-ScOutputEventsWrapper)
    - [SearchBlocksFilter](#massa-api-v1-SearchBlocksFilter)
    - [SearchBlocksRequest](#massa-api-v1-SearchBlocksRequest)
    - [SearchBlocksResponse](#massa-api-v1-SearchBlocksResponse)
    - [SearchEndorsementsFilter](#massa-api-v1-SearchEndorsementsFilter)
    - [SearchEndorsementsRequest](#massa-api-v1-SearchEndorsementsRequest)
    - [SearchEndorsementsResponse](#massa-api-v1-SearchEndorsementsResponse)
    - [SearchOperationsFilter](#massa-api-v1-SearchOperationsFilter)
    - [SearchOperationsRequest](#massa-api-v1-SearchOperationsRequest)
    - [SearchOperationsResponse](#massa-api-v1-SearchOperationsResponse)
    - [SelectorDrawsFilter](#massa-api-v1-SelectorDrawsFilter)
    - [SendBlocksRequest](#massa-api-v1-SendBlocksRequest)
    - [SendBlocksResponse](#massa-api-v1-SendBlocksResponse)
    - [SendEndorsementsRequest](#massa-api-v1-SendEndorsementsRequest)
    - [SendEndorsementsResponse](#massa-api-v1-SendEndorsementsResponse)
    - [SendOperationsRequest](#massa-api-v1-SendOperationsRequest)
    - [SendOperationsResponse](#massa-api-v1-SendOperationsResponse)
    - [SlotABICallStacks](#massa-api-v1-SlotABICallStacks)
    - [StakersFilter](#massa-api-v1-StakersFilter)
    - [TransactionsThroughputRequest](#massa-api-v1-TransactionsThroughputRequest)
    - [TransactionsThroughputResponse](#massa-api-v1-TransactionsThroughputResponse)
    - [TransactionsThroughputServerRequest](#massa-api-v1-TransactionsThroughputServerRequest)
    - [TransactionsThroughputServerResponse](#massa-api-v1-TransactionsThroughputServerResponse)
    - [TransferInfo](#massa-api-v1-TransferInfo)
    - [TransferInfos](#massa-api-v1-TransferInfos)
  
    - [ExecutionQueryExecutionStatus](#massa-api-v1-ExecutionQueryExecutionStatus)
    - [FinalityLevel](#massa-api-v1-FinalityLevel)
  
    - [PublicService](#massa-api-v1-PublicService)
  
- [Scalar Value Types](#scalar-value-types)



<a name="massa_api_v1_private-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## massa/api/v1/private.proto



<a name="massa-api-v1-AddStakingSecretKeysRequest"></a>

### AddStakingSecretKeysRequest
AddStakingSecretKeysRequest holds the request for AddStakingSecretKeys


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| secret_keys | [string](#string) | repeated | Secret keys to add to wallet |






<a name="massa-api-v1-AddStakingSecretKeysResponse"></a>

### AddStakingSecretKeysResponse
AddStakingSecretKeysResponse holds the response from AddStakingSecretKeys






<a name="massa-api-v1-AddToBootstrapBlacklistRequest"></a>

### AddToBootstrapBlacklistRequest
AddToBootstrapBlacklistRequest holds the request for AddToBootstrapBlacklist


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ips | [string](#string) | repeated | IP addresses to add to bootstrap blacklist |






<a name="massa-api-v1-AddToBootstrapBlacklistResponse"></a>

### AddToBootstrapBlacklistResponse
AddToBootstrapBlacklistResponse holds the response from AddToBootstrapBlacklist






<a name="massa-api-v1-AddToBootstrapWhitelistRequest"></a>

### AddToBootstrapWhitelistRequest
AddToBootstrapWhitelistRequest holds the request for AddToBootstrapWhitelist


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ips | [string](#string) | repeated | IP addresses to add to bootstrap whitelist |






<a name="massa-api-v1-AddToBootstrapWhitelistResponse"></a>

### AddToBootstrapWhitelistResponse
AddToBootstrapWhitelistResponse holds the response from AddToBootstrapWhitelist






<a name="massa-api-v1-AddToPeersWhitelistRequest"></a>

### AddToPeersWhitelistRequest
AddToPeersWhitelistRequest holds the request for AddToPeersWhitelist


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ips | [string](#string) | repeated | IP addresses to add to peers whitelist |






<a name="massa-api-v1-AddToPeersWhitelistResponse"></a>

### AddToPeersWhitelistResponse
AddToPeersWhitelistResponse holds the response from AddToPeersWhitelist






<a name="massa-api-v1-AllowEveryoneToBootstrapRequest"></a>

### AllowEveryoneToBootstrapRequest
AllowEveryoneToBootstrapRequest holds the request for AllowEveryoneToBootstrap






<a name="massa-api-v1-AllowEveryoneToBootstrapResponse"></a>

### AllowEveryoneToBootstrapResponse
AllowEveryoneToBootstrapResponse holds the response from AllowEveryoneToBootstrap






<a name="massa-api-v1-BanNodesByIdsRequest"></a>

### BanNodesByIdsRequest
BanNodesByIdsRequest holds the request for BanNodesByIds


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_ids | [string](#string) | repeated | Node ids to ban |






<a name="massa-api-v1-BanNodesByIdsResponse"></a>

### BanNodesByIdsResponse
BanNodesByIdsResponse holds the response from BanNodesByIds






<a name="massa-api-v1-BanNodesByIpsRequest"></a>

### BanNodesByIpsRequest
BanNodesByIpsRequest holds the request for BanNodesByIps


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ips | [string](#string) | repeated | Node IP addresses to ban |






<a name="massa-api-v1-BanNodesByIpsResponse"></a>

### BanNodesByIpsResponse
BanNodesByIpsResponse holds the response from BanNodesByIps






<a name="massa-api-v1-GetBootstrapBlacklistRequest"></a>

### GetBootstrapBlacklistRequest
GetBootstrapBlacklistRequest holds the request for GetBootstrapBlacklist






<a name="massa-api-v1-GetBootstrapBlacklistResponse"></a>

### GetBootstrapBlacklistResponse
GetBootstrapBlacklistResponse holds the response from GetBootstrapBlacklist


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ips | [string](#string) | repeated | Bootstrap blacklisted IP addresses |






<a name="massa-api-v1-GetBootstrapWhitelistRequest"></a>

### GetBootstrapWhitelistRequest
GetBootstrapWhitelistRequest holds the request for GetBootstrapWhitelist






<a name="massa-api-v1-GetBootstrapWhitelistResponse"></a>

### GetBootstrapWhitelistResponse
GetBootstrapWhitelistResponse holds the response from GetBootstrapWhitelist


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ips | [string](#string) | repeated | Bootstrap whitelisted IP addresses |






<a name="massa-api-v1-GetMipStatusRequest"></a>

### GetMipStatusRequest
GetMipStatusRequest holds request for GetMipStatus






<a name="massa-api-v1-GetMipStatusResponse"></a>

### GetMipStatusResponse
GetMipStatusResponse holds response from GetMipStatus


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mipstatus_entries | [massa.model.v1.MipStatusEntry](#massa-model-v1-MipStatusEntry) | repeated | (MipInfo - status id) entries |






<a name="massa-api-v1-GetNodeStatusRequest"></a>

### GetNodeStatusRequest
GetNodeStatusRequest holds the request for GetNodeStatus






<a name="massa-api-v1-GetNodeStatusResponse"></a>

### GetNodeStatusResponse
GetNodeStatusResponse holds the response from GetNodeStatus


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [massa.model.v1.NodeStatus](#massa-model-v1-NodeStatus) |  | Node status |






<a name="massa-api-v1-GetPeersWhitelistRequest"></a>

### GetPeersWhitelistRequest
GetPeersWhitelistRequest holds the request for GetPeersWhitelist






<a name="massa-api-v1-GetPeersWhitelistResponse"></a>

### GetPeersWhitelistResponse
GetPeersWhitelistResponse holds the response from GetPeersWhitelist


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ips | [string](#string) | repeated | Whitelisted IP addresses |






<a name="massa-api-v1-RemoveFromBootstrapBlacklistRequest"></a>

### RemoveFromBootstrapBlacklistRequest
RemoveFromBootstrapBlacklistRequest holds the request for RemoveFromBootstrapBlacklist


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ips | [string](#string) | repeated | IP addresses to remove from bootstrap blacklist |






<a name="massa-api-v1-RemoveFromBootstrapBlacklistResponse"></a>

### RemoveFromBootstrapBlacklistResponse
RemoveFromBootstrapBlacklistResponse holds the response from RemoveFromBootstrapBlacklist






<a name="massa-api-v1-RemoveFromBootstrapWhitelistRequest"></a>

### RemoveFromBootstrapWhitelistRequest
RemoveFromBootstrapWhitelistRequest holds the request for RemoveFromBootstrapWhitelist


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ips | [string](#string) | repeated | IP addresses to remove from bootstrap whitelist |






<a name="massa-api-v1-RemoveFromBootstrapWhitelistResponse"></a>

### RemoveFromBootstrapWhitelistResponse
RemoveFromBootstrapWhitelistResponse holds the response from RemoveFromBootstrapWhitelist






<a name="massa-api-v1-RemoveFromPeersWhitelistRequest"></a>

### RemoveFromPeersWhitelistRequest
RemoveFromPeersWhitelistRequest holds the request for RemoveFromPeersWhitelist


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ips | [string](#string) | repeated | IP addresses to remove from peers whitelist |






<a name="massa-api-v1-RemoveFromPeersWhitelistResponse"></a>

### RemoveFromPeersWhitelistResponse
RemoveFromPeersWhitelistResponse holds the response from RemoveFromPeersWhitelist






<a name="massa-api-v1-RemoveStakingAddressesRequest"></a>

### RemoveStakingAddressesRequest
RemoveStakingAddressesRequest holds the request for RemoveStakingAddresses


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| addresses | [string](#string) | repeated | Addresses to remove from staking |






<a name="massa-api-v1-RemoveStakingAddressesResponse"></a>

### RemoveStakingAddressesResponse
RemoveStakingAddressesResponse holds the response from RemoveStakingAddresses






<a name="massa-api-v1-ShutdownGracefullyRequest"></a>

### ShutdownGracefullyRequest
ShutdownGracefullyRequest holds the request for ShutdownGracefully






<a name="massa-api-v1-ShutdownGracefullyResponse"></a>

### ShutdownGracefullyResponse
ShutdownGracefullyResponse holds the response from ShutdownGracefully






<a name="massa-api-v1-SignMessagesRequest"></a>

### SignMessagesRequest
SignMessagesRequest holds the request for SignMessages


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| messages | [bytes](#bytes) | repeated | Messages to sign in bytes |






<a name="massa-api-v1-SignMessagesResponse"></a>

### SignMessagesResponse
SignMessagesResponse holds the response from SignMessages


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| public_key | [string](#string) |  | Public key that signed the message |
| signatures | [string](#string) | repeated | Signatures |






<a name="massa-api-v1-UnbanNodesByIdsRequest"></a>

### UnbanNodesByIdsRequest
UnbanNodesByIdsRequest holds the request for UnbanNodesByIds


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| node_ids | [string](#string) | repeated | Node ids to unban |






<a name="massa-api-v1-UnbanNodesByIdsResponse"></a>

### UnbanNodesByIdsResponse
UnbanNodesByIdsResponse holds the response from UnbanNodesByIds






<a name="massa-api-v1-UnbanNodesByIpsRequest"></a>

### UnbanNodesByIpsRequest
UnbanNodesByIpsRequest holds the request for UnbanNodesByIps


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ips | [string](#string) | repeated | Nodes IP addresses to unban |






<a name="massa-api-v1-UnbanNodesByIpsResponse"></a>

### UnbanNodesByIpsResponse
UnbanNodesByIpsResponse holds the response from UnbanNodesByIps





 

 

 


<a name="massa-api-v1-PrivateService"></a>

### PrivateService
Massa private gRPC service

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| AddToBootstrapBlacklist | [AddToBootstrapBlacklistRequest](#massa-api-v1-AddToBootstrapBlacklistRequest) | [AddToBootstrapBlacklistResponse](#massa-api-v1-AddToBootstrapBlacklistResponse) | Add IP addresses to node bootstrap blacklist |
| AddToBootstrapWhitelist | [AddToBootstrapWhitelistRequest](#massa-api-v1-AddToBootstrapWhitelistRequest) | [AddToBootstrapWhitelistResponse](#massa-api-v1-AddToBootstrapWhitelistResponse) | Add IP addresses to node bootstrap whitelist |
| AddToPeersWhitelist | [AddToPeersWhitelistRequest](#massa-api-v1-AddToPeersWhitelistRequest) | [AddToPeersWhitelistResponse](#massa-api-v1-AddToPeersWhitelistResponse) | Add IP addresses to node peers whitelist. No confirmation to expect. Note: If the ip was unknown it adds it to the known peers, otherwise it updates the peer type |
| AddStakingSecretKeys | [AddStakingSecretKeysRequest](#massa-api-v1-AddStakingSecretKeysRequest) | [AddStakingSecretKeysResponse](#massa-api-v1-AddStakingSecretKeysResponse) | Add staking secret keys to wallet |
| AllowEveryoneToBootstrap | [AllowEveryoneToBootstrapRequest](#massa-api-v1-AllowEveryoneToBootstrapRequest) | [AllowEveryoneToBootstrapResponse](#massa-api-v1-AllowEveryoneToBootstrapResponse) | Allow everyone to bootstrap from the node by removing bootstrap whitelist configuration file |
| GetBootstrapBlacklist | [GetBootstrapBlacklistRequest](#massa-api-v1-GetBootstrapBlacklistRequest) | [GetBootstrapBlacklistResponse](#massa-api-v1-GetBootstrapBlacklistResponse) | Get node bootstrap blacklist IP addresses |
| GetBootstrapWhitelist | [GetBootstrapWhitelistRequest](#massa-api-v1-GetBootstrapWhitelistRequest) | [GetBootstrapWhitelistResponse](#massa-api-v1-GetBootstrapWhitelistResponse) | Get node bootstrap whitelist IP addresses |
| GetMipStatus | [GetMipStatusRequest](#massa-api-v1-GetMipStatusRequest) | [GetMipStatusResponse](#massa-api-v1-GetMipStatusResponse) | Get Mip status |
| GetNodeStatus | [GetNodeStatusRequest](#massa-api-v1-GetNodeStatusRequest) | [GetNodeStatusResponse](#massa-api-v1-GetNodeStatusResponse) | Get node status |
| GetPeersWhitelist | [GetPeersWhitelistRequest](#massa-api-v1-GetPeersWhitelistRequest) | [GetPeersWhitelistResponse](#massa-api-v1-GetPeersWhitelistResponse) | Get node peers whitelist IP addresses |
| BanNodesByIds | [BanNodesByIdsRequest](#massa-api-v1-BanNodesByIdsRequest) | [BanNodesByIdsResponse](#massa-api-v1-BanNodesByIdsResponse) | Ban multiple nodes by their individual ids |
| BanNodesByIps | [BanNodesByIpsRequest](#massa-api-v1-BanNodesByIpsRequest) | [BanNodesByIpsResponse](#massa-api-v1-BanNodesByIpsResponse) | Ban multiple nodes by their individual IP addresses |
| RemoveFromBootstrapBlacklist | [RemoveFromBootstrapBlacklistRequest](#massa-api-v1-RemoveFromBootstrapBlacklistRequest) | [RemoveFromBootstrapBlacklistResponse](#massa-api-v1-RemoveFromBootstrapBlacklistResponse) | Remove from bootstrap blacklist given IP addresses |
| RemoveFromBootstrapWhitelist | [RemoveFromBootstrapWhitelistRequest](#massa-api-v1-RemoveFromBootstrapWhitelistRequest) | [RemoveFromBootstrapWhitelistResponse](#massa-api-v1-RemoveFromBootstrapWhitelistResponse) | Remove from bootstrap whitelist given IP addresses |
| RemoveFromPeersWhitelist | [RemoveFromPeersWhitelistRequest](#massa-api-v1-RemoveFromPeersWhitelistRequest) | [RemoveFromPeersWhitelistResponse](#massa-api-v1-RemoveFromPeersWhitelistResponse) | Remove from peers whitelist given IP addresses |
| RemoveStakingAddresses | [RemoveStakingAddressesRequest](#massa-api-v1-RemoveStakingAddressesRequest) | [RemoveStakingAddressesResponse](#massa-api-v1-RemoveStakingAddressesResponse) | Remove addresses from staking |
| SignMessages | [SignMessagesRequest](#massa-api-v1-SignMessagesRequest) | [SignMessagesResponse](#massa-api-v1-SignMessagesResponse) | Sign messages with node&#39;s key |
| ShutdownGracefully | [ShutdownGracefullyRequest](#massa-api-v1-ShutdownGracefullyRequest) | [ShutdownGracefullyResponse](#massa-api-v1-ShutdownGracefullyResponse) | Shutdown the node gracefully |
| UnbanNodesByIds | [UnbanNodesByIdsRequest](#massa-api-v1-UnbanNodesByIdsRequest) | [UnbanNodesByIdsResponse](#massa-api-v1-UnbanNodesByIdsResponse) | Unban multiple nodes by their individual ids |
| UnbanNodesByIps | [UnbanNodesByIpsRequest](#massa-api-v1-UnbanNodesByIpsRequest) | [UnbanNodesByIpsResponse](#massa-api-v1-UnbanNodesByIpsResponse) | Unban multiple nodes by their individual IP addresses |

 



<a name="massa_api_v1_public-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## massa/api/v1/public.proto



<a name="massa-api-v1-ABICallStack"></a>

### ABICallStack
Definition of an ABI call stack


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| call_stack | [ABICallStackElementParent](#massa-api-v1-ABICallStackElementParent) | repeated | All elements of the call stack |






<a name="massa-api-v1-ABICallStackElement"></a>

### ABICallStackElement
Definition of an ABI call stack element


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | name of the ABI |
| parameters | [string](#string) | repeated | Parameters of the ABI |
| return_value | [string](#string) |  | Return value of the ABI |






<a name="massa-api-v1-ABICallStackElementCall"></a>

### ABICallStackElementCall
Definition of an ABI call stack element that is the &#39;call&#39; ABI


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | name of the ABI |
| parameters | [string](#string) | repeated | Parameters of the ABI |
| sub_calls | [ABICallStackElementParent](#massa-api-v1-ABICallStackElementParent) | repeated | Calls made within this SC call |
| return_value | [string](#string) |  | Return value of the ABI |






<a name="massa-api-v1-ABICallStackElementParent"></a>

### ABICallStackElementParent
Definition of an ABI call stack element parent


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| element | [ABICallStackElement](#massa-api-v1-ABICallStackElement) |  | Any ABI call that is not the ABI &#39;call&#39; |
| element_call | [ABICallStackElementCall](#massa-api-v1-ABICallStackElementCall) |  | Element that is the ABI &#39;call&#39; |






<a name="massa-api-v1-ASCABICallStack"></a>

### ASCABICallStack
ABI asynchronous execution call stack


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| index | [uint64](#uint64) |  | Index of the execution in the slot |
| call_stack | [ABICallStackElementParent](#massa-api-v1-ABICallStackElementParent) | repeated | Call stack |






<a name="massa-api-v1-AddressBalanceCandidate"></a>

### AddressBalanceCandidate
Request to get the balance (candidate) of an address


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address to query |






<a name="massa-api-v1-AddressBalanceFinal"></a>

### AddressBalanceFinal
Request to get the balance (final) of an address


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address to query |






<a name="massa-api-v1-AddressBytecodeCandidate"></a>

### AddressBytecodeCandidate
Request to get the bytecode (candidate) of an address


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address to query |






<a name="massa-api-v1-AddressBytecodeFinal"></a>

### AddressBytecodeFinal
Request to get the bytecode (final) of an address


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address to query |






<a name="massa-api-v1-AddressDatastoreKeysCandidate"></a>

### AddressDatastoreKeysCandidate
Request to get the datastore keys (candidate) of an address


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address to query |
| prefix | [bytes](#bytes) |  | Prefix for the keys |
| start_key | [google.protobuf.BytesValue](#google-protobuf-BytesValue) |  | Key offset for the search |
| inclusive_start_key | [google.protobuf.BoolValue](#google-protobuf-BoolValue) |  | included start_key |
| end_key | [google.protobuf.BytesValue](#google-protobuf-BytesValue) |  | End key for the search |
| inclusive_end_key | [google.protobuf.BoolValue](#google-protobuf-BoolValue) |  | included end_key |
| limit | [google.protobuf.UInt32Value](#google-protobuf-UInt32Value) |  | Limit for the number of keys |






<a name="massa-api-v1-AddressDatastoreKeysFinal"></a>

### AddressDatastoreKeysFinal
Request to get the datastore keys (final) of an address


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address to query |
| prefix | [bytes](#bytes) |  | Prefix for the keys |
| start_key | [google.protobuf.BytesValue](#google-protobuf-BytesValue) |  | Key offset for the search |
| inclusive_start_key | [google.protobuf.BoolValue](#google-protobuf-BoolValue) |  | included start_key |
| end_key | [google.protobuf.BytesValue](#google-protobuf-BytesValue) |  | End key for the search |
| inclusive_end_key | [google.protobuf.BoolValue](#google-protobuf-BoolValue) |  | included end_key |
| limit | [google.protobuf.UInt32Value](#google-protobuf-UInt32Value) |  | Limit for the number of keys |






<a name="massa-api-v1-AddressDatastoreValueCandidate"></a>

### AddressDatastoreValueCandidate
Request to get a datastore value (candidate) for an address


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address to query |
| key | [bytes](#bytes) |  | Key for the value |






<a name="massa-api-v1-AddressDatastoreValueFinal"></a>

### AddressDatastoreValueFinal
Request to get a datastore value (final) for an address


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address to query |
| key | [bytes](#bytes) |  | Key for the value |






<a name="massa-api-v1-AddressDeferredCreditsCandidate"></a>

### AddressDeferredCreditsCandidate
Request to get the deferred credits (candidate) of an address


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address to query |






<a name="massa-api-v1-AddressDeferredCreditsFinal"></a>

### AddressDeferredCreditsFinal
Request to get the deferred credits (final) of an address


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address to query |






<a name="massa-api-v1-AddressExistsCandidate"></a>

### AddressExistsCandidate
Request to check if address exists (candidate)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address to check |






<a name="massa-api-v1-AddressExistsFinal"></a>

### AddressExistsFinal
Request to check if address exists (final)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address to check |






<a name="massa-api-v1-AddressRollsCandidate"></a>

### AddressRollsCandidate
Request to get the roll count (candidate) of an address


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address to query |






<a name="massa-api-v1-AddressRollsFinal"></a>

### AddressRollsFinal
Request to get the roll count (final) of an address


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address to query |






<a name="massa-api-v1-AsyncPoolChangesFilter"></a>

### AsyncPoolChangesFilter
AsyncPoolChangesFilter


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| none | [massa.model.v1.Empty](#massa-model-v1-Empty) |  | Do not return any message |
| type | [massa.model.v1.AsyncPoolChangeType](#massa-model-v1-AsyncPoolChangeType) |  | The type of the change |
| handler | [string](#string) |  | The handler function name within the destination address bytecode |
| destination_address | [string](#string) |  | The address towards which the message is being sent |
| emitter_address | [string](#string) |  | The address that sent the message |
| can_be_executed | [bool](#bool) |  | Boolean that determine if the message can be executed. For messages without filter this boolean is always true. For messages with filter, this boolean is true if the filter has been matched between `validity_start` and current slot. |






<a name="massa-api-v1-CycleInfos"></a>

### CycleInfos
Request to get all information for a given cycle


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cycle | [uint64](#uint64) |  | Cycle to query |
| restrict_to_addresses | [string](#string) | repeated | Addresses to restrict the query (if None, info for all addresses will be returned) |






<a name="massa-api-v1-DeferredCallABICallStack"></a>

### DeferredCallABICallStack
Operation execution call stack


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deferred_call_id | [string](#string) |  | Deferred call id |
| call_stack | [ABICallStackElementParent](#massa-api-v1-ABICallStackElementParent) | repeated | Call stack |






<a name="massa-api-v1-DeferredCallInfo"></a>

### DeferredCallInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| call_id | [string](#string) |  |  |






<a name="massa-api-v1-DeferredCallInfoEntry"></a>

### DeferredCallInfoEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sender_address | [string](#string) |  |  |
| target_slot | [massa.model.v1.Slot](#massa-model-v1-Slot) |  |  |
| target_address | [string](#string) |  |  |
| target_function | [string](#string) |  |  |
| parameters | [bytes](#bytes) |  |  |
| coins | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  |  |
| max_gas | [uint64](#uint64) |  |  |
| fee | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  |  |
| cancelled | [bool](#bool) |  |  |






<a name="massa-api-v1-DeferredCallInfoResponse"></a>

### DeferredCallInfoResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| call_id | [string](#string) |  |  |
| call | [DeferredCallInfoEntry](#massa-api-v1-DeferredCallInfoEntry) |  |  |






<a name="massa-api-v1-DeferredCallQuote"></a>

### DeferredCallQuote
Deferred call quote


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| target_slot | [massa.model.v1.Slot](#massa-model-v1-Slot) |  | target slot |
| max_gas | [uint64](#uint64) |  | max gas requested |
| params_size | [uint64](#uint64) |  | params size in bytes |






<a name="massa-api-v1-DeferredCallQuoteResponse"></a>

### DeferredCallQuoteResponse
deferred call quote response


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| target_slot | [massa.model.v1.Slot](#massa-model-v1-Slot) |  | the slot requested |
| max_gas_request | [uint64](#uint64) |  | The gas requested |
| available | [bool](#bool) |  | if the quote is available |
| price | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | The amount |






<a name="massa-api-v1-DeferredCallsBySlot"></a>

### DeferredCallsBySlot



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot | [massa.model.v1.Slot](#massa-model-v1-Slot) |  |  |






<a name="massa-api-v1-DeferredCallsBySlotResponse"></a>

### DeferredCallsBySlotResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot | [massa.model.v1.Slot](#massa-model-v1-Slot) |  |  |
| call_ids | [string](#string) | repeated |  |






<a name="massa-api-v1-DeferredCreditsEntry"></a>

### DeferredCreditsEntry
Deferred credits entry


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot | [massa.model.v1.Slot](#massa-model-v1-Slot) |  | Slot |
| amount | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | Amount |






<a name="massa-api-v1-DeferredCreditsEntryWrapper"></a>

### DeferredCreditsEntryWrapper
Deferred credits entry wrapper


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entries | [DeferredCreditsEntry](#massa-api-v1-DeferredCreditsEntry) | repeated | Deferred credits entry |






<a name="massa-api-v1-DenunciationExecutionStatusCandidate"></a>

### DenunciationExecutionStatusCandidate
Request to get the execution status (candidate) for a denunciation


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| denunciation_index | [massa.model.v1.DenunciationIndex](#massa-model-v1-DenunciationIndex) |  | Denunciation index to query |






<a name="massa-api-v1-DenunciationExecutionStatusFinal"></a>

### DenunciationExecutionStatusFinal
Request to get the execution status (final) for a denunciation


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| denunciation_index | [massa.model.v1.DenunciationIndex](#massa-model-v1-DenunciationIndex) |  | Denunciation index to query |






<a name="massa-api-v1-Events"></a>

### Events
Request to get filtered events


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filters | [ScExecutionEventsFilter](#massa-api-v1-ScExecutionEventsFilter) | repeated | Returns all the events that verify all the filters |






<a name="massa-api-v1-ExecuteReadOnlyCallRequest"></a>

### ExecuteReadOnlyCallRequest
ExecuteReadOnlyCallRequest holds request for ExecuteReadOnlyCall


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| call | [massa.model.v1.ReadOnlyExecutionCall](#massa-model-v1-ReadOnlyExecutionCall) |  | Execution call |






<a name="massa-api-v1-ExecuteReadOnlyCallResponse"></a>

### ExecuteReadOnlyCallResponse
ExecuteReadOnlyCallResponse holds response from ExecuteReadOnlyCall


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| output | [massa.model.v1.ReadOnlyExecutionOutput](#massa-model-v1-ReadOnlyExecutionOutput) |  | Execution output |






<a name="massa-api-v1-ExecutedDenounciationFilter"></a>

### ExecutedDenounciationFilter
ExecutedDenounciationFilter


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| none | [massa.model.v1.Empty](#massa-model-v1-Empty) |  | Do not return any message |






<a name="massa-api-v1-ExecutedOpsChangesFilter"></a>

### ExecutedOpsChangesFilter
ExecutedOpsChangesFilter


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| none | [massa.model.v1.Empty](#massa-model-v1-Empty) |  | Do not return any message |
| operation_id | [string](#string) |  | Operation id |






<a name="massa-api-v1-ExecutionEventFilter"></a>

### ExecutionEventFilter
ExecutionEventFilter


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| none | [massa.model.v1.Empty](#massa-model-v1-Empty) |  | Do not return any message |
| caller_address | [string](#string) |  | Caller address |
| emitter_address | [string](#string) |  | Emitter address |
| original_operation_id | [string](#string) |  | Original operation id |
| is_failure | [bool](#bool) |  | Whether the event is a failure |






<a name="massa-api-v1-ExecutionQueryCycleInfos"></a>

### ExecutionQueryCycleInfos
Cycle information for execution query


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cycle | [uint64](#uint64) |  | Cycle number |
| is_final | [bool](#bool) |  | Whether the cycle is final |
| staker_infos | [ExecutionQueryStakerInfoEntry](#massa-api-v1-ExecutionQueryStakerInfoEntry) | repeated | Infos for each PoS-participating address among the ones that were asked |






<a name="massa-api-v1-ExecutionQueryRequestItem"></a>

### ExecutionQueryRequestItem
Query state query item


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address_exists_candidate | [AddressExistsCandidate](#massa-api-v1-AddressExistsCandidate) |  | Checks if address exists (candidate) |
| address_exists_final | [AddressExistsFinal](#massa-api-v1-AddressExistsFinal) |  | Checks if address exists (final) |
| address_balance_candidate | [AddressBalanceCandidate](#massa-api-v1-AddressBalanceCandidate) |  | Gets the balance (candidate) of an address |
| address_balance_final | [AddressBalanceFinal](#massa-api-v1-AddressBalanceFinal) |  | Gets the balance (final) of an address |
| address_bytecode_candidate | [AddressBytecodeCandidate](#massa-api-v1-AddressBytecodeCandidate) |  | Gets the bytecode (candidate) of an address |
| address_bytecode_final | [AddressBytecodeFinal](#massa-api-v1-AddressBytecodeFinal) |  | Gets the bytecode (final) of an address |
| address_datastore_keys_candidate | [AddressDatastoreKeysCandidate](#massa-api-v1-AddressDatastoreKeysCandidate) |  | Gets the datastore keys (candidate) of an address |
| address_datastore_keys_final | [AddressDatastoreKeysFinal](#massa-api-v1-AddressDatastoreKeysFinal) |  | Gets the datastore keys (final) of an address |
| address_datastore_value_candidate | [AddressDatastoreValueCandidate](#massa-api-v1-AddressDatastoreValueCandidate) |  | Gets a datastore value (candidate) for an address |
| address_datastore_value_final | [AddressDatastoreValueFinal](#massa-api-v1-AddressDatastoreValueFinal) |  | Gets a datastore value (final) for an address |
| op_execution_status_candidate | [OpExecutionStatusCandidate](#massa-api-v1-OpExecutionStatusCandidate) |  | Gets the execution status (candidate) for an operation |
| op_execution_status_final | [OpExecutionStatusFinal](#massa-api-v1-OpExecutionStatusFinal) |  | Gets the execution status (final) for an operation |
| denunciation_execution_status_candidate | [DenunciationExecutionStatusCandidate](#massa-api-v1-DenunciationExecutionStatusCandidate) |  | Gets the execution status (candidate) for a denunciation |
| denunciation_execution_status_final | [DenunciationExecutionStatusFinal](#massa-api-v1-DenunciationExecutionStatusFinal) |  | Gets the execution status (final) for a denunciation |
| address_rolls_candidate | [AddressRollsCandidate](#massa-api-v1-AddressRollsCandidate) |  | Gets the roll count (candidate) of an address |
| address_rolls_final | [AddressRollsFinal](#massa-api-v1-AddressRollsFinal) |  | Gets the roll count (final) of an address |
| address_deferred_credits_candidate | [AddressDeferredCreditsCandidate](#massa-api-v1-AddressDeferredCreditsCandidate) |  | Gets the deferred credits (candidate) of an address |
| address_deferred_credits_final | [AddressDeferredCreditsFinal](#massa-api-v1-AddressDeferredCreditsFinal) |  | Gets the deferred credits (final) of an address |
| cycle_infos | [CycleInfos](#massa-api-v1-CycleInfos) |  | Gets all information for a given cycle |
| events | [Events](#massa-api-v1-Events) |  | Gets filtered events |
| deferred_call_quote | [DeferredCallQuote](#massa-api-v1-DeferredCallQuote) |  | Deferred call quote |
| deferred_call_info | [DeferredCallInfo](#massa-api-v1-DeferredCallInfo) |  | Deferred calls info |
| deferred_calls_by_slot | [DeferredCallsBySlot](#massa-api-v1-DeferredCallsBySlot) |  | Deferred calls by slot |






<a name="massa-api-v1-ExecutionQueryResponse"></a>

### ExecutionQueryResponse
Execution state query response


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [ExecutionQueryResponseItem](#massa-api-v1-ExecutionQueryResponseItem) |  | Execution query response item |
| error | [massa.model.v1.Error](#massa-model-v1-Error) |  | Massa error |






<a name="massa-api-v1-ExecutionQueryResponseItem"></a>

### ExecutionQueryResponseItem
Execution state query response item


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| boolean | [bool](#bool) |  | Boolean value |
| roll_count | [uint64](#uint64) |  | Roll counts value |
| amount | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | Amount value |
| bytes | [bytes](#bytes) |  | Bytes value |
| vec_bytes | [massa.model.v1.ArrayOfBytesWrapper](#massa-model-v1-ArrayOfBytesWrapper) |  | Vector of bytes value |
| deferred_credits | [DeferredCreditsEntryWrapper](#massa-api-v1-DeferredCreditsEntryWrapper) |  | Deferred credits value |
| execution_status | [ExecutionQueryExecutionStatus](#massa-api-v1-ExecutionQueryExecutionStatus) |  | Execution status value |
| cycle_infos | [ExecutionQueryCycleInfos](#massa-api-v1-ExecutionQueryCycleInfos) |  | Cycle infos value |
| events | [ScOutputEventsWrapper](#massa-api-v1-ScOutputEventsWrapper) |  | Events |
| deferred_call_quote | [DeferredCallQuoteResponse](#massa-api-v1-DeferredCallQuoteResponse) |  | Deferred call quote |
| deferred_call_info | [DeferredCallInfoResponse](#massa-api-v1-DeferredCallInfoResponse) |  | Deferred call info |
| deferred_calls_by_slot | [DeferredCallsBySlotResponse](#massa-api-v1-DeferredCallsBySlotResponse) |  | Deferred calls by slot |






<a name="massa-api-v1-ExecutionQueryStakerInfo"></a>

### ExecutionQueryStakerInfo
Staker information for execution query


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| active_rolls | [uint64](#uint64) |  | Active roll count |
| production_stats | [ExecutionQueryStakerInfoProductionStatsEntry](#massa-api-v1-ExecutionQueryStakerInfoProductionStatsEntry) |  | Production stats |






<a name="massa-api-v1-ExecutionQueryStakerInfoEntry"></a>

### ExecutionQueryStakerInfoEntry
Staker information for a given cycle


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address of the staker |
| info | [ExecutionQueryStakerInfo](#massa-api-v1-ExecutionQueryStakerInfo) |  | Staker info |






<a name="massa-api-v1-ExecutionQueryStakerInfoProductionStats"></a>

### ExecutionQueryStakerInfoProductionStats
Production statistics for staker info in execution query


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_success_count | [uint64](#uint64) |  | Production successes |
| block_failure_count | [uint64](#uint64) |  | Production failures |






<a name="massa-api-v1-ExecutionQueryStakerInfoProductionStatsEntry"></a>

### ExecutionQueryStakerInfoProductionStatsEntry
ExecutionQueryStakerInfoProductionStats entry


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address of the staker |
| stats | [ExecutionQueryStakerInfoProductionStats](#massa-api-v1-ExecutionQueryStakerInfoProductionStats) |  | Production failure |






<a name="massa-api-v1-GetBlocksRequest"></a>

### GetBlocksRequest
GetBlocksRequest holds request for GetBlocks


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_ids | [string](#string) | repeated | Block ids |






<a name="massa-api-v1-GetBlocksResponse"></a>

### GetBlocksResponse
GetBlocksResponse holds response from GetBlocks


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| wrapped_blocks | [massa.model.v1.BlockWrapper](#massa-model-v1-BlockWrapper) | repeated | Wrapped blocks |






<a name="massa-api-v1-GetDatastoreEntriesRequest"></a>

### GetDatastoreEntriesRequest
GetDatastoreEntriesRequest holds request from GetDatastoreEntries


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filters | [GetDatastoreEntryFilter](#massa-api-v1-GetDatastoreEntryFilter) | repeated | Returns all the datastore entries that verify all the filters |






<a name="massa-api-v1-GetDatastoreEntriesResponse"></a>

### GetDatastoreEntriesResponse
GetDatastoreEntriesResponse holds response from GetDatastoreEntries


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| datastore_entries | [massa.model.v1.DatastoreEntry](#massa-model-v1-DatastoreEntry) | repeated | Datastore entries |






<a name="massa-api-v1-GetDatastoreEntryFilter"></a>

### GetDatastoreEntryFilter
DatastoreEntryFilter


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address_key | [massa.model.v1.AddressKeyEntry](#massa-model-v1-AddressKeyEntry) |  | One of these (address-key) pairs |






<a name="massa-api-v1-GetEndorsementsRequest"></a>

### GetEndorsementsRequest
GetEndorsementsRequest holds request for GetEndorsements


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| endorsement_ids | [string](#string) | repeated | Endorsement ids |






<a name="massa-api-v1-GetEndorsementsResponse"></a>

### GetEndorsementsResponse
GetEndorsementsResponse holds response from GetEndorsements


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| wrapped_endorsements | [massa.model.v1.EndorsementWrapper](#massa-model-v1-EndorsementWrapper) | repeated | Wrapped operations |






<a name="massa-api-v1-GetNextBlockBestParentsRequest"></a>

### GetNextBlockBestParentsRequest
GetNextBlockBestParentsRequest holds request for GetNextBlockBestParents






<a name="massa-api-v1-GetNextBlockBestParentsResponse"></a>

### GetNextBlockBestParentsResponse
GetNextBlockBestParentsResponse holds response from GetNextBlockBestParents


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_parents | [massa.model.v1.BlockParent](#massa-model-v1-BlockParent) | repeated | Next block best parents |






<a name="massa-api-v1-GetOperationABICallStacksRequest"></a>

### GetOperationABICallStacksRequest
GetOperationABICallStacks request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| operation_ids | [string](#string) | repeated | Operations ids to get the call stack from |






<a name="massa-api-v1-GetOperationABICallStacksResponse"></a>

### GetOperationABICallStacksResponse
GetOperationABICallStacks response


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| call_stacks | [ABICallStack](#massa-api-v1-ABICallStack) | repeated |  |






<a name="massa-api-v1-GetOperationsRequest"></a>

### GetOperationsRequest
GetOperationsRequest holds request for GetOperations


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| operation_ids | [string](#string) | repeated | Operation ids |






<a name="massa-api-v1-GetOperationsResponse"></a>

### GetOperationsResponse
GetOperationsResponse holds response from GetOperations


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| wrapped_operations | [massa.model.v1.OperationWrapper](#massa-model-v1-OperationWrapper) | repeated | Wrapped operations |






<a name="massa-api-v1-GetScExecutionEventsRequest"></a>

### GetScExecutionEventsRequest
GetScExecutionEventsRequest holds request for GetScExecutionEvents


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filters | [ScExecutionEventsFilter](#massa-api-v1-ScExecutionEventsFilter) | repeated | Returns all the sc execution events that verify all the filters |






<a name="massa-api-v1-GetScExecutionEventsResponse"></a>

### GetScExecutionEventsResponse
GetScExecutionEventsResponse holds response from GetScExecutionEvents


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| events | [massa.model.v1.ScExecutionEvent](#massa-model-v1-ScExecutionEvent) | repeated | ScExecutionEvents |






<a name="massa-api-v1-GetSelectorDrawsRequest"></a>

### GetSelectorDrawsRequest
GetSelectorDrawsRequest holds request from GetSelectorDraws


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filters | [SelectorDrawsFilter](#massa-api-v1-SelectorDrawsFilter) | repeated | Returns all the selector draws that verify all the filters |






<a name="massa-api-v1-GetSelectorDrawsResponse"></a>

### GetSelectorDrawsResponse
GetSelectorDrawsResponse holds response from GetSelectorDraws


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| draws | [massa.model.v1.SlotDraw](#massa-model-v1-SlotDraw) | repeated | Selector draws |






<a name="massa-api-v1-GetSlotABICallStacksRequest"></a>

### GetSlotABICallStacksRequest
GetSlotABICallStacks request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slots | [massa.model.v1.Slot](#massa-model-v1-Slot) | repeated | Slots asked |






<a name="massa-api-v1-GetSlotABICallStacksResponse"></a>

### GetSlotABICallStacksResponse
GetSlotABICallStacks response


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot_call_stacks | [SlotABICallStacks](#massa-api-v1-SlotABICallStacks) | repeated | Call stacks for the slots |






<a name="massa-api-v1-GetSlotTransfersRequest"></a>

### GetSlotTransfersRequest
GetSlotTransfersRequest holds request for GetSlotTransfers


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slots | [massa.model.v1.Slot](#massa-model-v1-Slot) | repeated | Slot to get the transfers from |






<a name="massa-api-v1-GetSlotTransfersResponse"></a>

### GetSlotTransfersResponse
GetSlotTransfersResponse holds response from GetSlotTransfers


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| transfer_each_slot | [TransferInfos](#massa-api-v1-TransferInfos) | repeated | Transfers for the slot |






<a name="massa-api-v1-GetStakersRequest"></a>

### GetStakersRequest
GetStakersRequest holds request from GetStakers


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filters | [StakersFilter](#massa-api-v1-StakersFilter) | repeated | Returns all the stakers that verify all the filters |






<a name="massa-api-v1-GetStakersResponse"></a>

### GetStakersResponse
GetStakersResponse holds response from GetStakers


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stakers | [massa.model.v1.StakerEntry](#massa-model-v1-StakerEntry) | repeated | Stakers |






<a name="massa-api-v1-GetStatusRequest"></a>

### GetStatusRequest
GetStatusRequest holds request from GetStatus






<a name="massa-api-v1-GetStatusResponse"></a>

### GetStatusResponse
GetStatusResponse holds request from GetStatus


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [massa.model.v1.PublicStatus](#massa-model-v1-PublicStatus) |  | Status |






<a name="massa-api-v1-GetTransactionsThroughputRequest"></a>

### GetTransactionsThroughputRequest
GetTransactionsThroughputRequest holds request for GetTransactionsThroughput






<a name="massa-api-v1-GetTransactionsThroughputResponse"></a>

### GetTransactionsThroughputResponse
GetTransactionsThroughputResponse holds response from
GetTransactionsThroughput


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| throughput | [uint32](#uint32) |  | Transactions throughput |






<a name="massa-api-v1-LedgerChangesFilter"></a>

### LedgerChangesFilter
LedgerChangesFilter


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| none | [massa.model.v1.Empty](#massa-model-v1-Empty) |  | Do not return any message |
| address | [string](#string) |  | Address for which we have ledger changes |






<a name="massa-api-v1-NewBlocksFilter"></a>

### NewBlocksFilter
NewBlocks Filter


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_ids | [massa.model.v1.BlockIds](#massa-model-v1-BlockIds) |  | One of these block ids |
| addresses | [massa.model.v1.Addresses](#massa-model-v1-Addresses) |  | One of these creator addresses |
| slot_range | [massa.model.v1.SlotRange](#massa-model-v1-SlotRange) |  | One of these slot ranges (inclusive) |






<a name="massa-api-v1-NewBlocksRequest"></a>

### NewBlocksRequest
NewBlocksRequest holds request for NewBlocks


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filters | [NewBlocksFilter](#massa-api-v1-NewBlocksFilter) | repeated | Returns all the blocks that verify all the filters |






<a name="massa-api-v1-NewBlocksResponse"></a>

### NewBlocksResponse
NewBlocksResponse holds response from NewBlocks


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| signed_block | [massa.model.v1.SignedBlock](#massa-model-v1-SignedBlock) |  | Signed block |






<a name="massa-api-v1-NewBlocksServerRequest"></a>

### NewBlocksServerRequest
NewBlocksServerRequest holds request for unidirectional NewBlocks


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filters | [NewBlocksFilter](#massa-api-v1-NewBlocksFilter) | repeated | Returns all the blocks that verify all the filters |






<a name="massa-api-v1-NewBlocksServerResponse"></a>

### NewBlocksServerResponse
NewBlocksServerResponse holds response from unidirectional NewBlocks


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| signed_block | [massa.model.v1.SignedBlock](#massa-model-v1-SignedBlock) |  | Signed block |






<a name="massa-api-v1-NewEndorsementsFilter"></a>

### NewEndorsementsFilter
NewEndorsements Filter


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| endorsement_ids | [massa.model.v1.EndorsementIds](#massa-model-v1-EndorsementIds) |  | One of these endorsement ids |
| addresses | [massa.model.v1.Addresses](#massa-model-v1-Addresses) |  | One of these creator addresses |
| block_ids | [massa.model.v1.BlockIds](#massa-model-v1-BlockIds) |  | One of these block ids |






<a name="massa-api-v1-NewEndorsementsRequest"></a>

### NewEndorsementsRequest
NewEndorsementsRequest holds request for NewEndorsements


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filters | [NewEndorsementsFilter](#massa-api-v1-NewEndorsementsFilter) | repeated | Returns all the endorsements that verify all the filters |






<a name="massa-api-v1-NewEndorsementsResponse"></a>

### NewEndorsementsResponse
NewEndorsementsResponse holds response from NewEndorsements


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| signed_endorsement | [massa.model.v1.SignedEndorsement](#massa-model-v1-SignedEndorsement) |  | Signed endorsement |






<a name="massa-api-v1-NewEndorsementsServerRequest"></a>

### NewEndorsementsServerRequest
NewEndorsementsServerRequest holds request for NewEndorsements


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filters | [NewEndorsementsFilter](#massa-api-v1-NewEndorsementsFilter) | repeated | Returns all the endorsements that verify all the filters |






<a name="massa-api-v1-NewEndorsementsServerResponse"></a>

### NewEndorsementsServerResponse
NewEndorsementsServerResponse holds response from NewEndorsements


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| signed_endorsement | [massa.model.v1.SignedEndorsement](#massa-model-v1-SignedEndorsement) |  | Signed endorsement |






<a name="massa-api-v1-NewFilledBlocksFilter"></a>

### NewFilledBlocksFilter
NewFilledBlocks Filter


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_ids | [massa.model.v1.BlockIds](#massa-model-v1-BlockIds) |  | One of these block ids |
| addresses | [massa.model.v1.Addresses](#massa-model-v1-Addresses) |  | One of these creator addresses |
| slot_range | [massa.model.v1.SlotRange](#massa-model-v1-SlotRange) |  | One of these slot ranges (inclusive) |






<a name="massa-api-v1-NewFilledBlocksRequest"></a>

### NewFilledBlocksRequest
NewFilledBlocksRequest holds request for NewFilledBlocks


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filters | [NewBlocksFilter](#massa-api-v1-NewBlocksFilter) | repeated | Returns all the blocks that verify one of the filters |






<a name="massa-api-v1-NewFilledBlocksResponse"></a>

### NewFilledBlocksResponse
NewFilledBlocksResponse holds response from NewFilledBlocks


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filled_block | [massa.model.v1.FilledBlock](#massa-model-v1-FilledBlock) |  | Block with operations content found in the node. |






<a name="massa-api-v1-NewFilledBlocksServerRequest"></a>

### NewFilledBlocksServerRequest
NewFilledBlocksServerRequest holds request for NewFilledBlocks


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filters | [NewBlocksFilter](#massa-api-v1-NewBlocksFilter) | repeated | Returns all the blocks that verify one of the filters |






<a name="massa-api-v1-NewFilledBlocksServerResponse"></a>

### NewFilledBlocksServerResponse
NewFilledBlocksServerResponse holds response from NewFilledBlocks


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filled_block | [massa.model.v1.FilledBlock](#massa-model-v1-FilledBlock) |  | Block with operations content found in the node. |






<a name="massa-api-v1-NewOperationsFilter"></a>

### NewOperationsFilter
NewOperations Filter


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| operation_ids | [massa.model.v1.OperationIds](#massa-model-v1-OperationIds) |  | One of the operation ids |
| addresses | [massa.model.v1.Addresses](#massa-model-v1-Addresses) |  | One of these creator addresses |
| operation_types | [massa.model.v1.OpTypes](#massa-model-v1-OpTypes) |  | One of the operation types |






<a name="massa-api-v1-NewOperationsRequest"></a>

### NewOperationsRequest
NewOperationsRequest holds request for NewOperations


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filters | [NewOperationsFilter](#massa-api-v1-NewOperationsFilter) | repeated | Returns all the operations that verify all the filters |






<a name="massa-api-v1-NewOperationsResponse"></a>

### NewOperationsResponse
NewOperationsResponse holds response from NewOperations


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| signed_operation | [massa.model.v1.SignedOperation](#massa-model-v1-SignedOperation) |  | Signed operation |






<a name="massa-api-v1-NewOperationsServerRequest"></a>

### NewOperationsServerRequest
NewOperationsRequest holds request for NewOperations


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filters | [NewOperationsFilter](#massa-api-v1-NewOperationsFilter) | repeated | Returns all the operations that verify all the filters |






<a name="massa-api-v1-NewOperationsServerResponse"></a>

### NewOperationsServerResponse
NewOperationsServerResponse holds response from NewOperations


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| signed_operation | [massa.model.v1.SignedOperation](#massa-model-v1-SignedOperation) |  | Signed operation |






<a name="massa-api-v1-NewSlotABICallStacksRequest"></a>

### NewSlotABICallStacksRequest
NewSlotABICallStacks request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| finality_level | [FinalityLevel](#massa-api-v1-FinalityLevel) |  | Finality level to receive informations from |






<a name="massa-api-v1-NewSlotABICallStacksResponse"></a>

### NewSlotABICallStacksResponse
NewSlotABICallStacks response


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot | [massa.model.v1.Slot](#massa-model-v1-Slot) |  | Finality level to receive informations from |
| asc_call_stacks | [ASCABICallStack](#massa-api-v1-ASCABICallStack) | repeated | Call stacks for asynchronous execution |
| operation_call_stacks | [OperationABICallStack](#massa-api-v1-OperationABICallStack) | repeated | Call stack for operations |
| deferred_call_stacks | [DeferredCallABICallStack](#massa-api-v1-DeferredCallABICallStack) | repeated | Call stack for deferred calls |






<a name="massa-api-v1-NewSlotExecutionOutputsFilter"></a>

### NewSlotExecutionOutputsFilter
NewSlotExecutionOutputs Filter


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [massa.model.v1.ExecutionOutputStatus](#massa-model-v1-ExecutionOutputStatus) |  | Execution output status |
| slot_range | [massa.model.v1.SlotRange](#massa-model-v1-SlotRange) |  | Slot range |
| async_pool_changes_filter | [AsyncPoolChangesFilter](#massa-api-v1-AsyncPoolChangesFilter) |  | AsyncPoolChangesFilter |
| executed_denounciation_filter | [ExecutedDenounciationFilter](#massa-api-v1-ExecutedDenounciationFilter) |  | ExecutedDenounciationFilter |
| event_filter | [ExecutionEventFilter](#massa-api-v1-ExecutionEventFilter) |  | Execution event filter |
| executed_ops_changes_filter | [ExecutedOpsChangesFilter](#massa-api-v1-ExecutedOpsChangesFilter) |  | ExecutedOpsChangesFilter |
| ledger_changes_filter | [LedgerChangesFilter](#massa-api-v1-LedgerChangesFilter) |  | LedgerChangesFilter |






<a name="massa-api-v1-NewSlotExecutionOutputsRequest"></a>

### NewSlotExecutionOutputsRequest
NewSlotExecutionOutputsRequest holds request for NewSlotExecutionOutputs


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filters | [NewSlotExecutionOutputsFilter](#massa-api-v1-NewSlotExecutionOutputsFilter) | repeated | Returns all the slot execution outputs that verify all the filters |






<a name="massa-api-v1-NewSlotExecutionOutputsResponse"></a>

### NewSlotExecutionOutputsResponse
NewSlotExecutionOutputsResponse holds response from NewSlotExecutionOutputs


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| output | [massa.model.v1.SlotExecutionOutput](#massa-model-v1-SlotExecutionOutput) |  | Slot execution output |






<a name="massa-api-v1-NewSlotExecutionOutputsServerRequest"></a>

### NewSlotExecutionOutputsServerRequest
NewSlotExecutionOutputsServerRequest holds request for
NewSlotExecutionOutputs


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filters | [NewSlotExecutionOutputsFilter](#massa-api-v1-NewSlotExecutionOutputsFilter) | repeated | Returns all the slot execution outputs that verify all the filters |






<a name="massa-api-v1-NewSlotExecutionOutputsServerResponse"></a>

### NewSlotExecutionOutputsServerResponse
NewSlotExecutionOutputsServerResponse holds response from
NewSlotExecutionOutputs


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| output | [massa.model.v1.SlotExecutionOutput](#massa-model-v1-SlotExecutionOutput) |  | Slot execution output |






<a name="massa-api-v1-NewSlotTransfersRequest"></a>

### NewSlotTransfersRequest
NewSlotTransfers request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| finality_level | [FinalityLevel](#massa-api-v1-FinalityLevel) |  | Finality level to receive informations from |






<a name="massa-api-v1-NewSlotTransfersResponse"></a>

### NewSlotTransfersResponse
NewSlotTransfers response


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot | [massa.model.v1.Slot](#massa-model-v1-Slot) |  | Finality level to receive informations from |
| transfers | [TransferInfo](#massa-api-v1-TransferInfo) | repeated | Transfers |






<a name="massa-api-v1-NewTransfersInfoServerRequest"></a>

### NewTransfersInfoServerRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [google.protobuf.StringValue](#google-protobuf-StringValue) |  | optional filter address |






<a name="massa-api-v1-NewTransfersInfoServerResponse"></a>

### NewTransfersInfoServerResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot | [massa.model.v1.Slot](#massa-model-v1-Slot) |  | executed slot |
| timestamp | [int64](#int64) |  | timestamp |
| block_id | [google.protobuf.StringValue](#google-protobuf-StringValue) |  | block id |
| transfers_info | [massa.model.v1.ExecTransferInfo](#massa-model-v1-ExecTransferInfo) | repeated | transfers info

// reward for block producer massa.model.v1.TargetAmount block_producer_reward = 1; // reward for endorsement creator repeated massa.model.v1.TargetAmount endorsement_creator_rewards = 2; // reward for endorsement target massa.model.v1.TargetAmount endorsement_target_reward = 3; // executed denunciations repeated massa.model.v1.DenunciationAddress denunciations = 4; // executed roll buy / roll sell repeated massa.model.v1.OperationTypeRoll operations = 5; // executed async messages repeated massa.model.v1.AsyncMessageExecution async_messages = 6; // executed deferred calls repeated massa.model.v1.DeferredCallExecution deferred_calls_messages = 7; // executed deferred credits repeated massa.model.v1.TargetAmount deferred_credits_execution = 8; // executed cancel async messages repeated massa.model.v1.TargetAmount cancel_async_message_execution = 9; // executed auto sell roll repeated massa.model.v1.TargetAmount auto_sell_execution = 10; |






<a name="massa-api-v1-OpExecutionStatusCandidate"></a>

### OpExecutionStatusCandidate
Request to get the execution status (candidate) for an operation


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| operation_id | [string](#string) |  | Operation ID to query |






<a name="massa-api-v1-OpExecutionStatusFinal"></a>

### OpExecutionStatusFinal
Request to get the execution status (final) for an operation


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| operation_id | [string](#string) |  | Operation ID to query |






<a name="massa-api-v1-OperationABICallStack"></a>

### OperationABICallStack
Operation execution call stack


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| operation_id | [string](#string) |  | Operation id |
| call_stack | [ABICallStackElementParent](#massa-api-v1-ABICallStackElementParent) | repeated | Call stack |






<a name="massa-api-v1-PosChangesFilter"></a>

### PosChangesFilter
PosChangesFilter


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| none | [massa.model.v1.Empty](#massa-model-v1-Empty) |  | Do not return any message |
| address | [string](#string) |  | Address for which we have roll changes |






<a name="massa-api-v1-QueryStateRequest"></a>

### QueryStateRequest
Request to atomically execute a batch of execution state queries


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| queries | [ExecutionQueryRequestItem](#massa-api-v1-ExecutionQueryRequestItem) | repeated | Execution queries |






<a name="massa-api-v1-QueryStateResponse"></a>

### QueryStateResponse
Response to atomically execute a batch of execution state queries


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| final_cursor | [massa.model.v1.Slot](#massa-model-v1-Slot) |  | Final cursor position |
| candidate_cursor | [massa.model.v1.Slot](#massa-model-v1-Slot) |  | Candidate cursor position |
| final_state_fingerprint | [string](#string) |  | The hash of the XOF final state hash |
| responses | [ExecutionQueryResponse](#massa-api-v1-ExecutionQueryResponse) | repeated | List of execution query response items |






<a name="massa-api-v1-ScExecutionEventsFilter"></a>

### ScExecutionEventsFilter
ScExecutionEvents Filter


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot_range | [massa.model.v1.SlotRange](#massa-model-v1-SlotRange) |  | Slot range |
| caller_address | [string](#string) |  | Caller address |
| emitter_address | [string](#string) |  | Emitter address |
| original_operation_id | [string](#string) |  | Original operation id |
| is_failure | [bool](#bool) |  | Whether the event is a failure |
| status | [massa.model.v1.ScExecutionEventStatus](#massa-model-v1-ScExecutionEventStatus) |  | Status |






<a name="massa-api-v1-ScOutputEventsWrapper"></a>

### ScOutputEventsWrapper
ScOutputEvents wrapper


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| events | [massa.model.v1.ScExecutionEvent](#massa-model-v1-ScExecutionEvent) | repeated | Events |






<a name="massa-api-v1-SearchBlocksFilter"></a>

### SearchBlocksFilter
SearchBlocks Filter


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_ids | [massa.model.v1.BlockIds](#massa-model-v1-BlockIds) |  | One of these block ids |
| addresses | [massa.model.v1.Addresses](#massa-model-v1-Addresses) |  | One of these creator addresses |
| slot_range | [massa.model.v1.SlotRange](#massa-model-v1-SlotRange) |  | One of these slot ranges (inclusive) |






<a name="massa-api-v1-SearchBlocksRequest"></a>

### SearchBlocksRequest
SearchBlocksRequest holds request for SearchBlocks


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filters | [SearchBlocksFilter](#massa-api-v1-SearchBlocksFilter) | repeated | Returns all the blocks that verify all the filters |






<a name="massa-api-v1-SearchBlocksResponse"></a>

### SearchBlocksResponse
SearchBlocksResponse holds response from SearchBlocks


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_infos | [massa.model.v1.BlockInfo](#massa-model-v1-BlockInfo) | repeated | Information about the blocks |






<a name="massa-api-v1-SearchEndorsementsFilter"></a>

### SearchEndorsementsFilter
SearchEndorsements Filter


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| endorsement_ids | [massa.model.v1.EndorsementIds](#massa-model-v1-EndorsementIds) |  | One of these endorsement ids |
| addresses | [massa.model.v1.Addresses](#massa-model-v1-Addresses) |  | One of these creator addresses |
| block_ids | [massa.model.v1.BlockIds](#massa-model-v1-BlockIds) |  | One of these block ids |






<a name="massa-api-v1-SearchEndorsementsRequest"></a>

### SearchEndorsementsRequest
SearchEndorsementsRequest holds request for SearchEndorsements


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filters | [SearchEndorsementsFilter](#massa-api-v1-SearchEndorsementsFilter) | repeated | Returns all the endorsements informations that verify all the filters |






<a name="massa-api-v1-SearchEndorsementsResponse"></a>

### SearchEndorsementsResponse
SearchEndorsementsResponse holds response from SearchEndorsements


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| endorsement_infos | [massa.model.v1.EndorsementInfo](#massa-model-v1-EndorsementInfo) | repeated | Information about the endorsements |






<a name="massa-api-v1-SearchOperationsFilter"></a>

### SearchOperationsFilter
SearchOperations Filter


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| operation_ids | [massa.model.v1.OperationIds](#massa-model-v1-OperationIds) |  | One of the operation ids |
| addresses | [massa.model.v1.Addresses](#massa-model-v1-Addresses) |  | One of these creator addresses |






<a name="massa-api-v1-SearchOperationsRequest"></a>

### SearchOperationsRequest
SearchOperationsRequest holds request for SearchOperations


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filters | [SearchOperationsFilter](#massa-api-v1-SearchOperationsFilter) | repeated | Returns all the operations that verify all the filters |






<a name="massa-api-v1-SearchOperationsResponse"></a>

### SearchOperationsResponse
SearchOperationsResponse holds response from SearchOperations


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| operation_infos | [massa.model.v1.OperationInfo](#massa-model-v1-OperationInfo) | repeated | Information about the operations |






<a name="massa-api-v1-SelectorDrawsFilter"></a>

### SelectorDrawsFilter
SelectorDraws Filter


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| addresses | [massa.model.v1.Addresses](#massa-model-v1-Addresses) |  | One of the Addresses |
| slot_range | [massa.model.v1.SlotRange](#massa-model-v1-SlotRange) |  | One of the Slot range |






<a name="massa-api-v1-SendBlocksRequest"></a>

### SendBlocksRequest
SendBlocksRequest holds parameters to SendBlocks


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block | [bytes](#bytes) |  | Secure shared block |






<a name="massa-api-v1-SendBlocksResponse"></a>

### SendBlocksResponse
SendBlocksResponse holds response from SendBlocks


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| block_id | [string](#string) |  | Block result |
| error | [massa.model.v1.Error](#massa-model-v1-Error) |  | Massa error |






<a name="massa-api-v1-SendEndorsementsRequest"></a>

### SendEndorsementsRequest
SendEndorsementsRequest holds parameters to SendEndorsements


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| endorsements | [bytes](#bytes) | repeated | Secure shared endorsements |






<a name="massa-api-v1-SendEndorsementsResponse"></a>

### SendEndorsementsResponse
SendEndorsementsResponse holds response from SendEndorsements


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| endorsement_ids | [massa.model.v1.EndorsementIds](#massa-model-v1-EndorsementIds) |  | Endorsement result |
| error | [massa.model.v1.Error](#massa-model-v1-Error) |  | Massa error |






<a name="massa-api-v1-SendOperationsRequest"></a>

### SendOperationsRequest
SendOperationsRequest holds parameters to SendOperations


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| operations | [bytes](#bytes) | repeated | Secured shared operations |






<a name="massa-api-v1-SendOperationsResponse"></a>

### SendOperationsResponse
SendOperationsResponse holds response from SendOperations


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| operation_ids | [massa.model.v1.OperationIds](#massa-model-v1-OperationIds) |  | Operation result |
| error | [massa.model.v1.Error](#massa-model-v1-Error) |  | Massa error |






<a name="massa-api-v1-SlotABICallStacks"></a>

### SlotABICallStacks
Call stack for a slot


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| asc_call_stacks | [ASCABICallStack](#massa-api-v1-ASCABICallStack) | repeated | Call stacks for asynchronous execution |
| operation_call_stacks | [OperationABICallStack](#massa-api-v1-OperationABICallStack) | repeated | Call stack for operations |
| deferred_call_stacks | [DeferredCallABICallStack](#massa-api-v1-DeferredCallABICallStack) | repeated | Call stacks for deferred calls |






<a name="massa-api-v1-StakersFilter"></a>

### StakersFilter
Stakers Filter


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| min_rolls | [uint64](#uint64) |  | Minimum rolls |
| max_rolls | [uint64](#uint64) |  | Maximum rolls |
| limit | [uint64](#uint64) |  | Limit |






<a name="massa-api-v1-TransactionsThroughputRequest"></a>

### TransactionsThroughputRequest
TransactionsThroughputRequest holds request for TransactionsThroughput


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| interval | [google.protobuf.UInt64Value](#google-protobuf-UInt64Value) |  | Timer interval in seconds (Optional). Defaults to 10s |






<a name="massa-api-v1-TransactionsThroughputResponse"></a>

### TransactionsThroughputResponse
TransactionsThroughputResponse holds response from TransactionsThroughput


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| throughput | [uint32](#uint32) |  | Transactions throughput per second |






<a name="massa-api-v1-TransactionsThroughputServerRequest"></a>

### TransactionsThroughputServerRequest
TransactionsThroughputServerRequest holds request for TransactionsThroughput


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| interval | [google.protobuf.UInt64Value](#google-protobuf-UInt64Value) |  | Timer interval in seconds (Optional). Defaults to 10s |






<a name="massa-api-v1-TransactionsThroughputServerResponse"></a>

### TransactionsThroughputServerResponse
TransactionsThroughputServerResponse holds response from
TransactionsThroughput


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| throughput | [uint32](#uint32) |  | Transactions throughput per second |






<a name="massa-api-v1-TransferInfo"></a>

### TransferInfo
Transfer info


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| from | [string](#string) |  | Sender |
| to | [string](#string) |  | Receiver |
| amount | [uint64](#uint64) |  | Amount in nMAS |
| operation_id | [string](#string) |  | Operation id |
| asc_index | [uint64](#uint64) |  | Asynchronous execution index |
| deferred_call_id | [string](#string) |  | Deferred call id |






<a name="massa-api-v1-TransferInfos"></a>

### TransferInfos
List of transfers for a slot


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot | [massa.model.v1.Slot](#massa-model-v1-Slot) |  | Slot |
| transfers | [TransferInfo](#massa-api-v1-TransferInfo) | repeated | Transfers |





 


<a name="massa-api-v1-ExecutionQueryExecutionStatus"></a>

### ExecutionQueryExecutionStatus
Execution status of an operation or denunciation

| Name | Number | Description |
| ---- | ------ | ----------- |
| EXECUTION_QUERY_EXECUTION_STATUS_UNSPECIFIED | 0 | Default enum value |
| EXECUTION_QUERY_EXECUTION_STATUS_ALREADY_EXECUTED_WITH_SUCCESS | 1 | The operation or denunciation was executed recently with success |
| EXECUTION_QUERY_EXECUTION_STATUS_ALREADY_EXECUTED_WITH_FAILURE | 2 | The operation or denunciation was executed recently with failure |
| EXECUTION_QUERY_EXECUTION_STATUS_EXECUTABLE_OR_EXPIRED | 3 | The operation or denunciation was not executed recently but can |



<a name="massa-api-v1-FinalityLevel"></a>

### FinalityLevel
Finality level to filter on in streams

| Name | Number | Description |
| ---- | ------ | ----------- |
| FINALITY_LEVEL_UNSPECIFIED | 0 | Unspecified (receive both) |
| FINALITY_LEVEL_CANDIDATE | 1 | Candidate level |
| FINALITY_LEVEL_FINAL | 2 | Final level |


 

 


<a name="massa-api-v1-PublicService"></a>

### PublicService
Massa public gRPC service

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| ExecuteReadOnlyCall | [ExecuteReadOnlyCallRequest](#massa-api-v1-ExecuteReadOnlyCallRequest) | [ExecuteReadOnlyCallResponse](#massa-api-v1-ExecuteReadOnlyCallResponse) | Execute read only call |
| GetBlocks | [GetBlocksRequest](#massa-api-v1-GetBlocksRequest) | [GetBlocksResponse](#massa-api-v1-GetBlocksResponse) | Get blocks by ids |
| GetDatastoreEntries | [GetDatastoreEntriesRequest](#massa-api-v1-GetDatastoreEntriesRequest) | [GetDatastoreEntriesResponse](#massa-api-v1-GetDatastoreEntriesResponse) | Get datastore entries |
| GetEndorsements | [GetEndorsementsRequest](#massa-api-v1-GetEndorsementsRequest) | [GetEndorsementsResponse](#massa-api-v1-GetEndorsementsResponse) | Get endorsements by ids |
| GetNextBlockBestParents | [GetNextBlockBestParentsRequest](#massa-api-v1-GetNextBlockBestParentsRequest) | [GetNextBlockBestParentsResponse](#massa-api-v1-GetNextBlockBestParentsResponse) | Get next block best parents |
| GetOperations | [GetOperationsRequest](#massa-api-v1-GetOperationsRequest) | [GetOperationsResponse](#massa-api-v1-GetOperationsResponse) | Get operations by ids |
| GetScExecutionEvents | [GetScExecutionEventsRequest](#massa-api-v1-GetScExecutionEventsRequest) | [GetScExecutionEventsResponse](#massa-api-v1-GetScExecutionEventsResponse) | Get smart contracts execution events |
| GetSelectorDraws | [GetSelectorDrawsRequest](#massa-api-v1-GetSelectorDrawsRequest) | [GetSelectorDrawsResponse](#massa-api-v1-GetSelectorDrawsResponse) | Get selector draws |
| GetStakers | [GetStakersRequest](#massa-api-v1-GetStakersRequest) | [GetStakersResponse](#massa-api-v1-GetStakersResponse) | Get stakers |
| GetStatus | [GetStatusRequest](#massa-api-v1-GetStatusRequest) | [GetStatusResponse](#massa-api-v1-GetStatusResponse) | Get status |
| GetTransactionsThroughput | [GetTransactionsThroughputRequest](#massa-api-v1-GetTransactionsThroughputRequest) | [GetTransactionsThroughputResponse](#massa-api-v1-GetTransactionsThroughputResponse) | Get transactions throughput |
| QueryState | [QueryStateRequest](#massa-api-v1-QueryStateRequest) | [QueryStateResponse](#massa-api-v1-QueryStateResponse) | Query state |
| SearchBlocks | [SearchBlocksRequest](#massa-api-v1-SearchBlocksRequest) | [SearchBlocksResponse](#massa-api-v1-SearchBlocksResponse) | Search blocks |
| SearchEndorsements | [SearchEndorsementsRequest](#massa-api-v1-SearchEndorsementsRequest) | [SearchEndorsementsResponse](#massa-api-v1-SearchEndorsementsResponse) | Search endorsements |
| SearchOperations | [SearchOperationsRequest](#massa-api-v1-SearchOperationsRequest) | [SearchOperationsResponse](#massa-api-v1-SearchOperationsResponse) | Search operations |
| GetOperationABICallStacks | [GetOperationABICallStacksRequest](#massa-api-v1-GetOperationABICallStacksRequest) | [GetOperationABICallStacksResponse](#massa-api-v1-GetOperationABICallStacksResponse) | Get ABI call stack of an operation |
| GetSlotABICallStacks | [GetSlotABICallStacksRequest](#massa-api-v1-GetSlotABICallStacksRequest) | [GetSlotABICallStacksResponse](#massa-api-v1-GetSlotABICallStacksResponse) | Get ABI call stack of all asynchronous executions and all operations for a given slot |
| GetSlotTransfers | [GetSlotTransfersRequest](#massa-api-v1-GetSlotTransfersRequest) | [GetSlotTransfersResponse](#massa-api-v1-GetSlotTransfersResponse) | Get all the transfers of MAS for a given slot |
| NewBlocks | [NewBlocksRequest](#massa-api-v1-NewBlocksRequest) stream | [NewBlocksResponse](#massa-api-v1-NewBlocksResponse) stream | New received and produced blocks |
| NewBlocksServer | [NewBlocksServerRequest](#massa-api-v1-NewBlocksServerRequest) | [NewBlocksServerResponse](#massa-api-v1-NewBlocksServerResponse) stream | unidirecitonnal |
| NewEndorsements | [NewEndorsementsRequest](#massa-api-v1-NewEndorsementsRequest) stream | [NewEndorsementsResponse](#massa-api-v1-NewEndorsementsResponse) stream | New received and produced endorsements |
| NewEndorsementsServer | [NewEndorsementsServerRequest](#massa-api-v1-NewEndorsementsServerRequest) | [NewEndorsementsServerResponse](#massa-api-v1-NewEndorsementsServerResponse) stream | New received and produced endorsements |
| NewFilledBlocks | [NewFilledBlocksRequest](#massa-api-v1-NewFilledBlocksRequest) stream | [NewFilledBlocksResponse](#massa-api-v1-NewFilledBlocksResponse) stream | New received and produced blocks with operations |
| NewFilledBlocksServer | [NewFilledBlocksServerRequest](#massa-api-v1-NewFilledBlocksServerRequest) | [NewFilledBlocksServerResponse](#massa-api-v1-NewFilledBlocksServerResponse) stream | New received and produced blocks with operations unidirectional |
| NewOperations | [NewOperationsRequest](#massa-api-v1-NewOperationsRequest) stream | [NewOperationsResponse](#massa-api-v1-NewOperationsResponse) stream | New received and produced operations |
| NewOperationsServer | [NewOperationsServerRequest](#massa-api-v1-NewOperationsServerRequest) | [NewOperationsServerResponse](#massa-api-v1-NewOperationsServerResponse) stream | unidirectional stream NewOperations |
| NewSlotExecutionOutputs | [NewSlotExecutionOutputsRequest](#massa-api-v1-NewSlotExecutionOutputsRequest) stream | [NewSlotExecutionOutputsResponse](#massa-api-v1-NewSlotExecutionOutputsResponse) stream | New received and slot execution events |
| NewSlotExecutionOutputsServer | [NewSlotExecutionOutputsServerRequest](#massa-api-v1-NewSlotExecutionOutputsServerRequest) | [NewSlotExecutionOutputsServerResponse](#massa-api-v1-NewSlotExecutionOutputsServerResponse) stream | unidirectional stream NewSlotExecutionOutputs |
| NewSlotABICallStacks | [NewSlotABICallStacksRequest](#massa-api-v1-NewSlotABICallStacksRequest) stream | [NewSlotABICallStacksResponse](#massa-api-v1-NewSlotABICallStacksResponse) stream | Call stack for each slot executed |
| NewSlotTransfers | [NewSlotTransfersRequest](#massa-api-v1-NewSlotTransfersRequest) stream | [NewSlotTransfersResponse](#massa-api-v1-NewSlotTransfersResponse) stream | Transfer list for each slot executed |
| SendBlocks | [SendBlocksRequest](#massa-api-v1-SendBlocksRequest) stream | [SendBlocksResponse](#massa-api-v1-SendBlocksResponse) stream | Send blocks |
| SendEndorsements | [SendEndorsementsRequest](#massa-api-v1-SendEndorsementsRequest) stream | [SendEndorsementsResponse](#massa-api-v1-SendEndorsementsResponse) stream | Send endorsements |
| SendOperations | [SendOperationsRequest](#massa-api-v1-SendOperationsRequest) stream | [SendOperationsResponse](#massa-api-v1-SendOperationsResponse) stream | Send operations |
| TransactionsThroughput | [TransactionsThroughputRequest](#massa-api-v1-TransactionsThroughputRequest) stream | [TransactionsThroughputResponse](#massa-api-v1-TransactionsThroughputResponse) stream | Transactions throughput |
| TransactionsThroughputServer | [TransactionsThroughputServerRequest](#massa-api-v1-TransactionsThroughputServerRequest) | [TransactionsThroughputServerResponse](#massa-api-v1-TransactionsThroughputServerResponse) stream | Transactions throughput unidirectional |
| NewTransfersInfoServer | [NewTransfersInfoServerRequest](#massa-api-v1-NewTransfersInfoServerRequest) | [NewTransfersInfoServerResponse](#massa-api-v1-NewTransfersInfoServerResponse) stream | New execution Info |

 



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

