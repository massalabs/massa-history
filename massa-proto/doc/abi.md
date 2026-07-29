# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [massa/abi/v1/abi.proto](#massa_abi_v1_abi-proto)
    - [AbiResponse](#massa-abi-v1-AbiResponse)
    - [AddNativeAmountRequest](#massa-abi-v1-AddNativeAmountRequest)
    - [AddNativeAmountResult](#massa-abi-v1-AddNativeAmountResult)
    - [AddressFromPubKeyRequest](#massa-abi-v1-AddressFromPubKeyRequest)
    - [AddressFromPubKeyResult](#massa-abi-v1-AddressFromPubKeyResult)
    - [AppendDsValueRequest](#massa-abi-v1-AppendDsValueRequest)
    - [AppendDsValueResult](#massa-abi-v1-AppendDsValueResult)
    - [Base58CheckToBytesRequest](#massa-abi-v1-Base58CheckToBytesRequest)
    - [Base58CheckToBytesResult](#massa-abi-v1-Base58CheckToBytesResult)
    - [BytesToBase58CheckRequest](#massa-abi-v1-BytesToBase58CheckRequest)
    - [BytesToBase58CheckResult](#massa-abi-v1-BytesToBase58CheckResult)
    - [CallRequest](#massa-abi-v1-CallRequest)
    - [CallResponse](#massa-abi-v1-CallResponse)
    - [CallerHasWriteAccessRequest](#massa-abi-v1-CallerHasWriteAccessRequest)
    - [CallerHasWriteAccessResult](#massa-abi-v1-CallerHasWriteAccessResult)
    - [ChainIdRequest](#massa-abi-v1-ChainIdRequest)
    - [ChainIdResult](#massa-abi-v1-ChainIdResult)
    - [CheckAddressRequest](#massa-abi-v1-CheckAddressRequest)
    - [CheckAddressResult](#massa-abi-v1-CheckAddressResult)
    - [CheckNativeAmountRequest](#massa-abi-v1-CheckNativeAmountRequest)
    - [CheckNativeAmountResult](#massa-abi-v1-CheckNativeAmountResult)
    - [CheckPubKeyRequest](#massa-abi-v1-CheckPubKeyRequest)
    - [CheckPubKeyResult](#massa-abi-v1-CheckPubKeyResult)
    - [CheckSigRequest](#massa-abi-v1-CheckSigRequest)
    - [CheckSigResult](#massa-abi-v1-CheckSigResult)
    - [CheckedAddNativeTimeRequest](#massa-abi-v1-CheckedAddNativeTimeRequest)
    - [CheckedAddNativeTimeResult](#massa-abi-v1-CheckedAddNativeTimeResult)
    - [CheckedDivRemNativeTimeRequest](#massa-abi-v1-CheckedDivRemNativeTimeRequest)
    - [CheckedDivRemNativeTimeResult](#massa-abi-v1-CheckedDivRemNativeTimeResult)
    - [CheckedScalarDivRemNativeTimeRequest](#massa-abi-v1-CheckedScalarDivRemNativeTimeRequest)
    - [CheckedScalarDivRemNativeTimeResult](#massa-abi-v1-CheckedScalarDivRemNativeTimeResult)
    - [CheckedScalarMulNativeTimeRequest](#massa-abi-v1-CheckedScalarMulNativeTimeRequest)
    - [CheckedScalarMulNativeTimeResult](#massa-abi-v1-CheckedScalarMulNativeTimeResult)
    - [CheckedSubNativeTimeRequest](#massa-abi-v1-CheckedSubNativeTimeRequest)
    - [CheckedSubNativeTimeResult](#massa-abi-v1-CheckedSubNativeTimeResult)
    - [CompareAddressRequest](#massa-abi-v1-CompareAddressRequest)
    - [CompareAddressResult](#massa-abi-v1-CompareAddressResult)
    - [CompareNativeAmountRequest](#massa-abi-v1-CompareNativeAmountRequest)
    - [CompareNativeAmountResult](#massa-abi-v1-CompareNativeAmountResult)
    - [CompareNativeTimeRequest](#massa-abi-v1-CompareNativeTimeRequest)
    - [CompareNativeTimeResult](#massa-abi-v1-CompareNativeTimeResult)
    - [ComparePubKeyRequest](#massa-abi-v1-ComparePubKeyRequest)
    - [ComparePubKeyResult](#massa-abi-v1-ComparePubKeyResult)
    - [CreateScRequest](#massa-abi-v1-CreateScRequest)
    - [CreateScResult](#massa-abi-v1-CreateScResult)
    - [DeferredCallCancelRequest](#massa-abi-v1-DeferredCallCancelRequest)
    - [DeferredCallCancelResult](#massa-abi-v1-DeferredCallCancelResult)
    - [DeferredCallExistsRequest](#massa-abi-v1-DeferredCallExistsRequest)
    - [DeferredCallExistsResult](#massa-abi-v1-DeferredCallExistsResult)
    - [DeferredCallQuoteRequest](#massa-abi-v1-DeferredCallQuoteRequest)
    - [DeferredCallQuoteResult](#massa-abi-v1-DeferredCallQuoteResult)
    - [DeferredCallRegisterRequest](#massa-abi-v1-DeferredCallRegisterRequest)
    - [DeferredCallRegisterResult](#massa-abi-v1-DeferredCallRegisterResult)
    - [DeleteDsEntryRequest](#massa-abi-v1-DeleteDsEntryRequest)
    - [DeleteDsEntryResult](#massa-abi-v1-DeleteDsEntryResult)
    - [DivRemNativeAmountRequest](#massa-abi-v1-DivRemNativeAmountRequest)
    - [DivRemNativeAmountResult](#massa-abi-v1-DivRemNativeAmountResult)
    - [DsEntryExistsRequest](#massa-abi-v1-DsEntryExistsRequest)
    - [DsEntryExistsResult](#massa-abi-v1-DsEntryExistsResult)
    - [Error](#massa-abi-v1-Error)
    - [EvmGetAddressFromPubkeyRequest](#massa-abi-v1-EvmGetAddressFromPubkeyRequest)
    - [EvmGetAddressFromPubkeyResult](#massa-abi-v1-EvmGetAddressFromPubkeyResult)
    - [EvmGetPubkeyFromSignatureRequest](#massa-abi-v1-EvmGetPubkeyFromSignatureRequest)
    - [EvmGetPubkeyFromSignatureResult](#massa-abi-v1-EvmGetPubkeyFromSignatureResult)
    - [EvmVerifySigRequest](#massa-abi-v1-EvmVerifySigRequest)
    - [EvmVerifySigResult](#massa-abi-v1-EvmVerifySigResult)
    - [FunctionExistsRequest](#massa-abi-v1-FunctionExistsRequest)
    - [FunctionExistsResult](#massa-abi-v1-FunctionExistsResult)
    - [GenerateEventRequest](#massa-abi-v1-GenerateEventRequest)
    - [GenerateEventResult](#massa-abi-v1-GenerateEventResult)
    - [GetAddressCategoryRequest](#massa-abi-v1-GetAddressCategoryRequest)
    - [GetAddressCategoryResult](#massa-abi-v1-GetAddressCategoryResult)
    - [GetAddressVersionRequest](#massa-abi-v1-GetAddressVersionRequest)
    - [GetAddressVersionResult](#massa-abi-v1-GetAddressVersionResult)
    - [GetBalanceRequest](#massa-abi-v1-GetBalanceRequest)
    - [GetBalanceResult](#massa-abi-v1-GetBalanceResult)
    - [GetBytecodeRequest](#massa-abi-v1-GetBytecodeRequest)
    - [GetBytecodeResult](#massa-abi-v1-GetBytecodeResult)
    - [GetCallCoinsRequest](#massa-abi-v1-GetCallCoinsRequest)
    - [GetCallCoinsResult](#massa-abi-v1-GetCallCoinsResult)
    - [GetCallStackRequest](#massa-abi-v1-GetCallStackRequest)
    - [GetCallStackResult](#massa-abi-v1-GetCallStackResult)
    - [GetCurrentSlotRequest](#massa-abi-v1-GetCurrentSlotRequest)
    - [GetCurrentSlotResult](#massa-abi-v1-GetCurrentSlotResult)
    - [GetDsKeysRequest](#massa-abi-v1-GetDsKeysRequest)
    - [GetDsKeysResult](#massa-abi-v1-GetDsKeysResult)
    - [GetDsValueRequest](#massa-abi-v1-GetDsValueRequest)
    - [GetDsValueResult](#massa-abi-v1-GetDsValueResult)
    - [GetNativeTimeRequest](#massa-abi-v1-GetNativeTimeRequest)
    - [GetNativeTimeResult](#massa-abi-v1-GetNativeTimeResult)
    - [GetOpDataRequest](#massa-abi-v1-GetOpDataRequest)
    - [GetOpDataResult](#massa-abi-v1-GetOpDataResult)
    - [GetOpKeysRequest](#massa-abi-v1-GetOpKeysRequest)
    - [GetOpKeysResult](#massa-abi-v1-GetOpKeysResult)
    - [GetOriginOperationIdRequest](#massa-abi-v1-GetOriginOperationIdRequest)
    - [GetOriginOperationIdResult](#massa-abi-v1-GetOriginOperationIdResult)
    - [GetOwnedAddressesRequest](#massa-abi-v1-GetOwnedAddressesRequest)
    - [GetOwnedAddressesResult](#massa-abi-v1-GetOwnedAddressesResult)
    - [GetPubKeyVersionRequest](#massa-abi-v1-GetPubKeyVersionRequest)
    - [GetPubKeyVersionResult](#massa-abi-v1-GetPubKeyVersionResult)
    - [GetRemainingGasRequest](#massa-abi-v1-GetRemainingGasRequest)
    - [GetRemainingGasResult](#massa-abi-v1-GetRemainingGasResult)
    - [GetSignatureVersionRequest](#massa-abi-v1-GetSignatureVersionRequest)
    - [GetSignatureVersionResult](#massa-abi-v1-GetSignatureVersionResult)
    - [HashBlake3Request](#massa-abi-v1-HashBlake3Request)
    - [HashBlake3Result](#massa-abi-v1-HashBlake3Result)
    - [HashSha256Request](#massa-abi-v1-HashSha256Request)
    - [HashSha256Result](#massa-abi-v1-HashSha256Result)
    - [IsAddressEoaRequest](#massa-abi-v1-IsAddressEoaRequest)
    - [IsAddressEoaResult](#massa-abi-v1-IsAddressEoaResult)
    - [Keccak256Request](#massa-abi-v1-Keccak256Request)
    - [Keccak256Result](#massa-abi-v1-Keccak256Result)
    - [LocalExecutionRequest](#massa-abi-v1-LocalExecutionRequest)
    - [LocalExecutionResponse](#massa-abi-v1-LocalExecutionResponse)
    - [NativeAmountFromStringRequest](#massa-abi-v1-NativeAmountFromStringRequest)
    - [NativeAmountFromStringResult](#massa-abi-v1-NativeAmountFromStringResult)
    - [NativeAmountToStringRequest](#massa-abi-v1-NativeAmountToStringRequest)
    - [NativeAmountToStringResult](#massa-abi-v1-NativeAmountToStringResult)
    - [OpEntryExistsRequest](#massa-abi-v1-OpEntryExistsRequest)
    - [OpEntryExistsResult](#massa-abi-v1-OpEntryExistsResult)
    - [RespResult](#massa-abi-v1-RespResult)
    - [ScalarDivRemNativeAmountRequest](#massa-abi-v1-ScalarDivRemNativeAmountRequest)
    - [ScalarDivRemNativeAmountResult](#massa-abi-v1-ScalarDivRemNativeAmountResult)
    - [ScalarMulNativeAmountRequest](#massa-abi-v1-ScalarMulNativeAmountRequest)
    - [ScalarMulNativeAmountResult](#massa-abi-v1-ScalarMulNativeAmountResult)
    - [SendAsyncMessageFilter](#massa-abi-v1-SendAsyncMessageFilter)
    - [SendAsyncMessageRequest](#massa-abi-v1-SendAsyncMessageRequest)
    - [SendAsyncMessageResult](#massa-abi-v1-SendAsyncMessageResult)
    - [SetBytecodeRequest](#massa-abi-v1-SetBytecodeRequest)
    - [SetBytecodeResult](#massa-abi-v1-SetBytecodeResult)
    - [SetDsValueRequest](#massa-abi-v1-SetDsValueRequest)
    - [SetDsValueResult](#massa-abi-v1-SetDsValueResult)
    - [SubNativeAmountRequest](#massa-abi-v1-SubNativeAmountRequest)
    - [SubNativeAmountResult](#massa-abi-v1-SubNativeAmountResult)
    - [TransferCoinsRequest](#massa-abi-v1-TransferCoinsRequest)
    - [TransferCoinsResult](#massa-abi-v1-TransferCoinsResult)
    - [UnsafeRandomRequest](#massa-abi-v1-UnsafeRandomRequest)
    - [UnsafeRandomResult](#massa-abi-v1-UnsafeRandomResult)
    - [VerifySigRequest](#massa-abi-v1-VerifySigRequest)
    - [VerifySigResult](#massa-abi-v1-VerifySigResult)
  
- [Scalar Value Types](#scalar-value-types)



<a name="massa_abi_v1_abi-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## massa/abi/v1/abi.proto



<a name="massa-abi-v1-AbiResponse"></a>

### AbiResponse
Generic message that encapsulate response from ABI calls.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| res | [RespResult](#massa-abi-v1-RespResult) |  | variant for success |
| error | [Error](#massa-abi-v1-Error) |  | variant for error |






<a name="massa-abi-v1-AddNativeAmountRequest"></a>

### AddNativeAmountRequest
Amount addition request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| amount1 | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | First amount to add |
| amount2 | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | Second amount to add |






<a name="massa-abi-v1-AddNativeAmountResult"></a>

### AddNativeAmountResult
Amount addition result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sum | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | Sum of amounts |






<a name="massa-abi-v1-AddressFromPubKeyRequest"></a>

### AddressFromPubKeyRequest
Address from public key request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pub_key | [string](#string) |  | Native public key |






<a name="massa-abi-v1-AddressFromPubKeyResult"></a>

### AddressFromPubKeyResult
Address from public key result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address |






<a name="massa-abi-v1-AppendDsValueRequest"></a>

### AppendDsValueRequest
Append data request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  | key |
| value | [bytes](#bytes) |  | value |
| address | [google.protobuf.StringValue](#google-protobuf-StringValue) | optional | Address to append data for, if none, use current address |






<a name="massa-abi-v1-AppendDsValueResult"></a>

### AppendDsValueResult
Append data result






<a name="massa-abi-v1-Base58CheckToBytesRequest"></a>

### Base58CheckToBytesRequest
base58 to bytes request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| base58_check | [string](#string) |  |  |






<a name="massa-abi-v1-Base58CheckToBytesResult"></a>

### Base58CheckToBytesResult
base58 to bytes result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| bytes | [bytes](#bytes) |  |  |






<a name="massa-abi-v1-BytesToBase58CheckRequest"></a>

### BytesToBase58CheckRequest
bytes to base58 request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| bytes | [bytes](#bytes) |  |  |






<a name="massa-abi-v1-BytesToBase58CheckResult"></a>

### BytesToBase58CheckResult
bytes to base58 result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| base58_check | [string](#string) |  |  |






<a name="massa-abi-v1-CallRequest"></a>

### CallRequest
Call SC request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| target_sc_address | [string](#string) |  | Address of the smart contract to call |
| target_function_name | [string](#string) |  | Function to call in the targeted smart contract |
| function_arg | [bytes](#bytes) |  | Argument to the function serialized in a byte array. |
| call_coins | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | call_coins is the amount of coins to pay for the call |






<a name="massa-abi-v1-CallResponse"></a>

### CallResponse
Call SC response


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  | Return_data is the return value of the function |






<a name="massa-abi-v1-CallerHasWriteAccessRequest"></a>

### CallerHasWriteAccessRequest
Caller has write access request






<a name="massa-abi-v1-CallerHasWriteAccessResult"></a>

### CallerHasWriteAccessResult
Caller has write access result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| has_write_access | [bool](#bool) |  | Caller has write access |






<a name="massa-abi-v1-ChainIdRequest"></a>

### ChainIdRequest
Chain id request






<a name="massa-abi-v1-ChainIdResult"></a>

### ChainIdResult
Chain id result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [uint64](#uint64) |  |  |






<a name="massa-abi-v1-CheckAddressRequest"></a>

### CheckAddressRequest
Check address request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| to_check | [string](#string) |  | Address to check |






<a name="massa-abi-v1-CheckAddressResult"></a>

### CheckAddressResult
Check address result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| is_valid | [bool](#bool) |  | Is address valid |






<a name="massa-abi-v1-CheckNativeAmountRequest"></a>

### CheckNativeAmountRequest
Check Amount request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| to_check | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | Amount to check |






<a name="massa-abi-v1-CheckNativeAmountResult"></a>

### CheckNativeAmountResult
Check Amount result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| is_valid | [bool](#bool) |  | Is Amount valid |






<a name="massa-abi-v1-CheckPubKeyRequest"></a>

### CheckPubKeyRequest
Check PubKey request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| to_check | [string](#string) |  | PubKey to check |






<a name="massa-abi-v1-CheckPubKeyResult"></a>

### CheckPubKeyResult
Check PubKey result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| is_valid | [bool](#bool) |  | Is PubKey valid |






<a name="massa-abi-v1-CheckSigRequest"></a>

### CheckSigRequest
Check Sig request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| to_check | [string](#string) |  | Sig to check |






<a name="massa-abi-v1-CheckSigResult"></a>

### CheckSigResult
Check Sig result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| is_valid | [bool](#bool) |  | Is Sig valid |






<a name="massa-abi-v1-CheckedAddNativeTimeRequest"></a>

### CheckedAddNativeTimeRequest
Time addition checked request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| left | [massa.model.v1.NativeTime](#massa-model-v1-NativeTime) |  | First time to add |
| right | [massa.model.v1.NativeTime](#massa-model-v1-NativeTime) |  | Second time to add |






<a name="massa-abi-v1-CheckedAddNativeTimeResult"></a>

### CheckedAddNativeTimeResult
Time addition checked result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sum | [massa.model.v1.NativeTime](#massa-model-v1-NativeTime) |  | Sum of times |






<a name="massa-abi-v1-CheckedDivRemNativeTimeRequest"></a>

### CheckedDivRemNativeTimeRequest
Time division checked request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| dividend | [massa.model.v1.NativeTime](#massa-model-v1-NativeTime) |  | Time to divide |
| divisor | [massa.model.v1.NativeTime](#massa-model-v1-NativeTime) |  | Divisor to divide by |






<a name="massa-abi-v1-CheckedDivRemNativeTimeResult"></a>

### CheckedDivRemNativeTimeResult
Time division checked result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| quotient | [uint64](#uint64) |  | Quotient of time and divisor |
| remainder | [massa.model.v1.NativeTime](#massa-model-v1-NativeTime) |  | Remainder of time and divisor |






<a name="massa-abi-v1-CheckedScalarDivRemNativeTimeRequest"></a>

### CheckedScalarDivRemNativeTimeRequest
Time scalar divrem checked request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| dividend | [massa.model.v1.NativeTime](#massa-model-v1-NativeTime) |  | Time to divide |
| divisor | [uint64](#uint64) |  | Divisor to divide by |






<a name="massa-abi-v1-CheckedScalarDivRemNativeTimeResult"></a>

### CheckedScalarDivRemNativeTimeResult
Time scalar divrem checked result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| quotient | [massa.model.v1.NativeTime](#massa-model-v1-NativeTime) |  | Quotient of time and divisor |
| remainder | [massa.model.v1.NativeTime](#massa-model-v1-NativeTime) |  | Remainder of time and divisor |






<a name="massa-abi-v1-CheckedScalarMulNativeTimeRequest"></a>

### CheckedScalarMulNativeTimeRequest
Time scalar mult checked request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| time | [massa.model.v1.NativeTime](#massa-model-v1-NativeTime) |  | Time to multiply |
| coefficient | [uint64](#uint64) |  | Coefficient to multiply by |






<a name="massa-abi-v1-CheckedScalarMulNativeTimeResult"></a>

### CheckedScalarMulNativeTimeResult
Time scalar mult checked result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| product | [massa.model.v1.NativeTime](#massa-model-v1-NativeTime) |  | Product of time and coefficient |






<a name="massa-abi-v1-CheckedSubNativeTimeRequest"></a>

### CheckedSubNativeTimeRequest
Time subtraction checked request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| left | [massa.model.v1.NativeTime](#massa-model-v1-NativeTime) |  | First time to subtract from |
| right | [massa.model.v1.NativeTime](#massa-model-v1-NativeTime) |  | Second time to subtract |






<a name="massa-abi-v1-CheckedSubNativeTimeResult"></a>

### CheckedSubNativeTimeResult
Time subtraction checked result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| difference | [massa.model.v1.NativeTime](#massa-model-v1-NativeTime) |  | Difference of times (left - right) |






<a name="massa-abi-v1-CompareAddressRequest"></a>

### CompareAddressRequest
Compare Address request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| left | [string](#string) |  | First address to compare |
| right | [string](#string) |  | Second address to compare |






<a name="massa-abi-v1-CompareAddressResult"></a>

### CompareAddressResult
Compare Address result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [massa.model.v1.ComparisonResult](#massa-model-v1-ComparisonResult) |  | Comparison result |






<a name="massa-abi-v1-CompareNativeAmountRequest"></a>

### CompareNativeAmountRequest
Compare NativeAmount request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| left | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | First amount to compare |
| right | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | Second amount to compare |






<a name="massa-abi-v1-CompareNativeAmountResult"></a>

### CompareNativeAmountResult
Compare NativeAmount result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [massa.model.v1.ComparisonResult](#massa-model-v1-ComparisonResult) |  | Comparison result |






<a name="massa-abi-v1-CompareNativeTimeRequest"></a>

### CompareNativeTimeRequest
Time comparison request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| left | [massa.model.v1.NativeTime](#massa-model-v1-NativeTime) |  | First time to compare |
| right | [massa.model.v1.NativeTime](#massa-model-v1-NativeTime) |  | Second time to compare |






<a name="massa-abi-v1-CompareNativeTimeResult"></a>

### CompareNativeTimeResult
Time comparison result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [massa.model.v1.ComparisonResult](#massa-model-v1-ComparisonResult) |  | Comparison result |






<a name="massa-abi-v1-ComparePubKeyRequest"></a>

### ComparePubKeyRequest
Compare PubKey request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| left | [string](#string) |  | First public key to compare |
| right | [string](#string) |  | Second public key to compare |






<a name="massa-abi-v1-ComparePubKeyResult"></a>

### ComparePubKeyResult
Compare PubKey result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [massa.model.v1.ComparisonResult](#massa-model-v1-ComparisonResult) |  | Comparison result |






<a name="massa-abi-v1-CreateScRequest"></a>

### CreateScRequest
Create SC request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| bytecode | [bytes](#bytes) |  | Bytecode is the compiled code of the smart contract |






<a name="massa-abi-v1-CreateScResult"></a>

### CreateScResult
Create SC result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sc_address | [string](#string) |  | Address of the just created smart contract |






<a name="massa-abi-v1-DeferredCallCancelRequest"></a>

### DeferredCallCancelRequest
Deferred call cancel request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| call_id | [google.protobuf.StringValue](#google-protobuf-StringValue) |  | call id |






<a name="massa-abi-v1-DeferredCallCancelResult"></a>

### DeferredCallCancelResult
Deferred call cancel result






<a name="massa-abi-v1-DeferredCallExistsRequest"></a>

### DeferredCallExistsRequest
deferred call exists request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| call_id | [google.protobuf.StringValue](#google-protobuf-StringValue) |  | deferred call id |






<a name="massa-abi-v1-DeferredCallExistsResult"></a>

### DeferredCallExistsResult
deferred call exists result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| call_exists | [bool](#bool) |  | call exists |






<a name="massa-abi-v1-DeferredCallQuoteRequest"></a>

### DeferredCallQuoteRequest
Deferred call quote


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| target_slot | [massa.model.v1.Slot](#massa-model-v1-Slot) |  | target slot |
| max_gas | [uint64](#uint64) |  | max gas requested |
| params_size | [uint64](#uint64) |  | params size in bytes |






<a name="massa-abi-v1-DeferredCallQuoteResult"></a>

### DeferredCallQuoteResult
Deferred call quote result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| available | [bool](#bool) |  | available |
| cost | [uint64](#uint64) |  | cost |






<a name="massa-abi-v1-DeferredCallRegisterRequest"></a>

### DeferredCallRegisterRequest
deferred call register request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| target_address | [string](#string) |  | target address |
| target_function | [string](#string) |  | target function |
| target_slot | [massa.model.v1.Slot](#massa-model-v1-Slot) |  | start slot |
| max_gas | [uint64](#uint64) |  | gas requested |
| params | [bytes](#bytes) |  | params of the call |
| coins | [uint64](#uint64) |  | coins |






<a name="massa-abi-v1-DeferredCallRegisterResult"></a>

### DeferredCallRegisterResult
deferred call register result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| call_id | [google.protobuf.StringValue](#google-protobuf-StringValue) |  | deferred call id |






<a name="massa-abi-v1-DeleteDsEntryRequest"></a>

### DeleteDsEntryRequest
Delete data store entry request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  | key |
| address | [google.protobuf.StringValue](#google-protobuf-StringValue) | optional | Address to delete data for, if none, use current address |






<a name="massa-abi-v1-DeleteDsEntryResult"></a>

### DeleteDsEntryResult
Delete data store entry result






<a name="massa-abi-v1-DivRemNativeAmountRequest"></a>

### DivRemNativeAmountRequest
Amount division request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| dividend | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | Amount to divide |
| divisor | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | Divisor to divide by |






<a name="massa-abi-v1-DivRemNativeAmountResult"></a>

### DivRemNativeAmountResult
Amount division result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| quotient | [uint64](#uint64) |  | Quotient of amount and divisor |
| remainder | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | Remainder of amount and divisor |






<a name="massa-abi-v1-DsEntryExistsRequest"></a>

### DsEntryExistsRequest
Data store entry exists request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  | key |
| address | [google.protobuf.StringValue](#google-protobuf-StringValue) | optional | Address to check data for, if none, use current address |






<a name="massa-abi-v1-DsEntryExistsResult"></a>

### DsEntryExistsResult
Data store entry exists result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| has_data | [bool](#bool) |  | has data |






<a name="massa-abi-v1-Error"></a>

### Error
Error message used in abi Response


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  | a string representing the error |






<a name="massa-abi-v1-EvmGetAddressFromPubkeyRequest"></a>

### EvmGetAddressFromPubkeyRequest
EVM get address from public key request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pub_key | [bytes](#bytes) |  | Public key to get address from |






<a name="massa-abi-v1-EvmGetAddressFromPubkeyResult"></a>

### EvmGetAddressFromPubkeyResult
EVM get address from public key result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [bytes](#bytes) |  | Address |






<a name="massa-abi-v1-EvmGetPubkeyFromSignatureRequest"></a>

### EvmGetPubkeyFromSignatureRequest
EVM get public key from signature request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | Signed hash to verify |
| sig | [bytes](#bytes) |  | Signature to verify |






<a name="massa-abi-v1-EvmGetPubkeyFromSignatureResult"></a>

### EvmGetPubkeyFromSignatureResult
EVM get public key from signature result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pub_key | [bytes](#bytes) |  | Public key |






<a name="massa-abi-v1-EvmVerifySigRequest"></a>

### EvmVerifySigRequest
EVM signature verification request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sig | [bytes](#bytes) |  | Signature to verify |
| message | [bytes](#bytes) |  | Message to verify |
| pub_key | [bytes](#bytes) |  | Public key to verify with |






<a name="massa-abi-v1-EvmVerifySigResult"></a>

### EvmVerifySigResult
EVM signature verification result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| is_verified | [bool](#bool) |  | Verification result |






<a name="massa-abi-v1-FunctionExistsRequest"></a>

### FunctionExistsRequest
Function exists request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| target_sc_address | [string](#string) |  | Address of the smart contract to call |
| function_name | [string](#string) |  | The name of the function to check the existance of |






<a name="massa-abi-v1-FunctionExistsResult"></a>

### FunctionExistsResult
Function exists result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| exists | [bool](#bool) |  | Exists is true if the function exists |






<a name="massa-abi-v1-GenerateEventRequest"></a>

### GenerateEventRequest
Generate event request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [bytes](#bytes) |  | Event |






<a name="massa-abi-v1-GenerateEventResult"></a>

### GenerateEventResult
Generate event resulst






<a name="massa-abi-v1-GetAddressCategoryRequest"></a>

### GetAddressCategoryRequest
Get the category of the address request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  |  |






<a name="massa-abi-v1-GetAddressCategoryResult"></a>

### GetAddressCategoryResult
Get the category of the address result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| category | [massa.model.v1.AddressCategory](#massa-model-v1-AddressCategory) |  |  |






<a name="massa-abi-v1-GetAddressVersionRequest"></a>

### GetAddressVersionRequest
Get the version of the address request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  |  |






<a name="massa-abi-v1-GetAddressVersionResult"></a>

### GetAddressVersionResult
Get the version of the address result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [uint64](#uint64) |  |  |






<a name="massa-abi-v1-GetBalanceRequest"></a>

### GetBalanceRequest
Get balance request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [google.protobuf.StringValue](#google-protobuf-StringValue) | optional | Address to get balance for, if none, use current address |






<a name="massa-abi-v1-GetBalanceResult"></a>

### GetBalanceResult
Get balance result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| balance | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | balance |






<a name="massa-abi-v1-GetBytecodeRequest"></a>

### GetBytecodeRequest
Get bytecode request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [google.protobuf.StringValue](#google-protobuf-StringValue) | optional | Address to get bytecode for, if none, use current address |






<a name="massa-abi-v1-GetBytecodeResult"></a>

### GetBytecodeResult
Get bytecode result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| bytecode | [bytes](#bytes) |  | Bytecode |






<a name="massa-abi-v1-GetCallCoinsRequest"></a>

### GetCallCoinsRequest
Get call coins request






<a name="massa-abi-v1-GetCallCoinsResult"></a>

### GetCallCoinsResult
Get call coins result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| coins | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | coins |






<a name="massa-abi-v1-GetCallStackRequest"></a>

### GetCallStackRequest
Get call stack request






<a name="massa-abi-v1-GetCallStackResult"></a>

### GetCallStackResult
Get call stack result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| calls | [string](#string) | repeated | call stack |






<a name="massa-abi-v1-GetCurrentSlotRequest"></a>

### GetCurrentSlotRequest
Get current slot request






<a name="massa-abi-v1-GetCurrentSlotResult"></a>

### GetCurrentSlotResult
Get current slot result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| slot | [massa.model.v1.Slot](#massa-model-v1-Slot) |  | Current slot |






<a name="massa-abi-v1-GetDsKeysRequest"></a>

### GetDsKeysRequest
Get keys request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| prefix | [bytes](#bytes) |  | keys prefix |
| address | [google.protobuf.StringValue](#google-protobuf-StringValue) | optional | Address to get keys for |






<a name="massa-abi-v1-GetDsKeysResult"></a>

### GetDsKeysResult
Get keys result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| keys | [bytes](#bytes) | repeated | keys |






<a name="massa-abi-v1-GetDsValueRequest"></a>

### GetDsValueRequest
Get data request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  | key |
| address | [google.protobuf.StringValue](#google-protobuf-StringValue) | optional | Address to get data for, if none, use current address |






<a name="massa-abi-v1-GetDsValueResult"></a>

### GetDsValueResult
Get data result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [bytes](#bytes) |  | value |






<a name="massa-abi-v1-GetNativeTimeRequest"></a>

### GetNativeTimeRequest
Get native time request






<a name="massa-abi-v1-GetNativeTimeResult"></a>

### GetNativeTimeResult
Get native time result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| time | [massa.model.v1.NativeTime](#massa-model-v1-NativeTime) |  | Native time |






<a name="massa-abi-v1-GetOpDataRequest"></a>

### GetOpDataRequest
Get op data request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  | key |






<a name="massa-abi-v1-GetOpDataResult"></a>

### GetOpDataResult
Get op data result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [bytes](#bytes) |  | value |






<a name="massa-abi-v1-GetOpKeysRequest"></a>

### GetOpKeysRequest
Get op keys request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| prefix | [bytes](#bytes) |  | keys prefix |






<a name="massa-abi-v1-GetOpKeysResult"></a>

### GetOpKeysResult
Get op keys result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| keys | [bytes](#bytes) | repeated | keys |






<a name="massa-abi-v1-GetOriginOperationIdRequest"></a>

### GetOriginOperationIdRequest
Get origin operation id request






<a name="massa-abi-v1-GetOriginOperationIdResult"></a>

### GetOriginOperationIdResult
Get origin operation id result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| operation_id | [google.protobuf.StringValue](#google-protobuf-StringValue) | optional |  |






<a name="massa-abi-v1-GetOwnedAddressesRequest"></a>

### GetOwnedAddressesRequest
Get owned addresses request






<a name="massa-abi-v1-GetOwnedAddressesResult"></a>

### GetOwnedAddressesResult
Get owned addresses result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| addresses | [string](#string) | repeated | owned addresses |






<a name="massa-abi-v1-GetPubKeyVersionRequest"></a>

### GetPubKeyVersionRequest
Get the version of the public key request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pub_key | [string](#string) |  |  |






<a name="massa-abi-v1-GetPubKeyVersionResult"></a>

### GetPubKeyVersionResult
Get the version of the public key result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [uint64](#uint64) |  |  |






<a name="massa-abi-v1-GetRemainingGasRequest"></a>

### GetRemainingGasRequest
Get remaining gas request






<a name="massa-abi-v1-GetRemainingGasResult"></a>

### GetRemainingGasResult
Get remaining gas result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| remaining_gas | [uint64](#uint64) |  | remaining gas |






<a name="massa-abi-v1-GetSignatureVersionRequest"></a>

### GetSignatureVersionRequest
Get the version of the signature request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| signature | [string](#string) |  |  |






<a name="massa-abi-v1-GetSignatureVersionResult"></a>

### GetSignatureVersionResult
Get the version of the signature result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [uint64](#uint64) |  |  |






<a name="massa-abi-v1-HashBlake3Request"></a>

### HashBlake3Request
Blake3 hash request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  | Data to hash |






<a name="massa-abi-v1-HashBlake3Result"></a>

### HashBlake3Result
Blake3 hash result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | Hash of data |






<a name="massa-abi-v1-HashSha256Request"></a>

### HashSha256Request
Hash sha256 request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  | data |






<a name="massa-abi-v1-HashSha256Result"></a>

### HashSha256Result
Hash sha256 result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | hash |






<a name="massa-abi-v1-IsAddressEoaRequest"></a>

### IsAddressEoaRequest
Is address EOA request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address | [string](#string) |  | Address to check |






<a name="massa-abi-v1-IsAddressEoaResult"></a>

### IsAddressEoaResult
Is address EOA result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| is_eoa | [bool](#bool) |  | Is address EOA |






<a name="massa-abi-v1-Keccak256Request"></a>

### Keccak256Request
Keccak256 hash request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  | Data to hash |






<a name="massa-abi-v1-Keccak256Result"></a>

### Keccak256Result
Keccak256 hash result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hash | [bytes](#bytes) |  | Hash of data |






<a name="massa-abi-v1-LocalExecutionRequest"></a>

### LocalExecutionRequest
Local execution request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| bytecode | [bytes](#bytes) |  | Bytecode is the compiled code of the smart contract |
| target_function_name | [string](#string) |  | Function to call in the targeted smart contract |
| function_arg | [bytes](#bytes) |  | Argument to the function serialized in a byte array. |






<a name="massa-abi-v1-LocalExecutionResponse"></a>

### LocalExecutionResponse
Local call response


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  | Return_data is the return value of the function |






<a name="massa-abi-v1-NativeAmountFromStringRequest"></a>

### NativeAmountFromStringRequest
Amount from string request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| to_convert | [string](#string) |  | String to convert to Amount |






<a name="massa-abi-v1-NativeAmountFromStringResult"></a>

### NativeAmountFromStringResult
Amount from string result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| converted_amount | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | Converted Amount |






<a name="massa-abi-v1-NativeAmountToStringRequest"></a>

### NativeAmountToStringRequest
Amount to string request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| to_convert | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | Amount to convert to string |






<a name="massa-abi-v1-NativeAmountToStringResult"></a>

### NativeAmountToStringResult
Amount to string result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| converted_amount | [string](#string) |  | Converted Amount |






<a name="massa-abi-v1-OpEntryExistsRequest"></a>

### OpEntryExistsRequest
Op entry exists request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  | key |






<a name="massa-abi-v1-OpEntryExistsResult"></a>

### OpEntryExistsResult
Op entry exists result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| has_key | [bool](#bool) |  | has key |






<a name="massa-abi-v1-RespResult"></a>

### RespResult
Tips to check for completeness exec:
`rg message | rg &#34;\{&#34; | rg &#34;Result&#34; | wc -l`
the given count should be equal to the number of messages in RespResult &#43; 1
the &#43;1 comes from ResResult itself which is counted above


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| add_native_amount_result | [AddNativeAmountResult](#massa-abi-v1-AddNativeAmountResult) |  |  |
| address_from_pub_key_result | [AddressFromPubKeyResult](#massa-abi-v1-AddressFromPubKeyResult) |  |  |
| append_ds_value_result | [AppendDsValueResult](#massa-abi-v1-AppendDsValueResult) |  |  |
| base58_check_to_bytes_result | [Base58CheckToBytesResult](#massa-abi-v1-Base58CheckToBytesResult) |  |  |
| bytes_to_base58_check_result | [BytesToBase58CheckResult](#massa-abi-v1-BytesToBase58CheckResult) |  |  |
| caller_has_write_access_result | [CallerHasWriteAccessResult](#massa-abi-v1-CallerHasWriteAccessResult) |  |  |
| check_address_result | [CheckAddressResult](#massa-abi-v1-CheckAddressResult) |  |  |
| check_native_amount_result | [CheckNativeAmountResult](#massa-abi-v1-CheckNativeAmountResult) |  |  |
| check_pub_key_result | [CheckPubKeyResult](#massa-abi-v1-CheckPubKeyResult) |  |  |
| check_sig_result | [CheckSigResult](#massa-abi-v1-CheckSigResult) |  |  |
| checked_add_native_time_result | [CheckedAddNativeTimeResult](#massa-abi-v1-CheckedAddNativeTimeResult) |  |  |
| checked_div_rem_native_time_result | [CheckedDivRemNativeTimeResult](#massa-abi-v1-CheckedDivRemNativeTimeResult) |  |  |
| checked_scalar_div_rem_native_time_result | [CheckedScalarDivRemNativeTimeResult](#massa-abi-v1-CheckedScalarDivRemNativeTimeResult) |  |  |
| checked_scalar_mul_native_time_result | [CheckedScalarMulNativeTimeResult](#massa-abi-v1-CheckedScalarMulNativeTimeResult) |  |  |
| checked_sub_native_time_result | [CheckedSubNativeTimeResult](#massa-abi-v1-CheckedSubNativeTimeResult) |  |  |
| compare_address_result | [CompareAddressResult](#massa-abi-v1-CompareAddressResult) |  |  |
| compare_native_amount_result | [CompareNativeAmountResult](#massa-abi-v1-CompareNativeAmountResult) |  |  |
| compare_native_time_result | [CompareNativeTimeResult](#massa-abi-v1-CompareNativeTimeResult) |  |  |
| compare_pub_key_result | [ComparePubKeyResult](#massa-abi-v1-ComparePubKeyResult) |  |  |
| create_sc_result | [CreateScResult](#massa-abi-v1-CreateScResult) |  |  |
| delete_ds_entry_result | [DeleteDsEntryResult](#massa-abi-v1-DeleteDsEntryResult) |  |  |
| div_rem_native_amount_result | [DivRemNativeAmountResult](#massa-abi-v1-DivRemNativeAmountResult) |  |  |
| ds_entry_exists_result | [DsEntryExistsResult](#massa-abi-v1-DsEntryExistsResult) |  |  |
| function_exists_result | [FunctionExistsResult](#massa-abi-v1-FunctionExistsResult) |  |  |
| generate_event_result | [GenerateEventResult](#massa-abi-v1-GenerateEventResult) |  |  |
| get_address_category_result | [GetAddressCategoryResult](#massa-abi-v1-GetAddressCategoryResult) |  |  |
| get_address_version_result | [GetAddressVersionResult](#massa-abi-v1-GetAddressVersionResult) |  |  |
| get_balance_result | [GetBalanceResult](#massa-abi-v1-GetBalanceResult) |  |  |
| get_bytecode_result | [GetBytecodeResult](#massa-abi-v1-GetBytecodeResult) |  |  |
| get_call_coins_result | [GetCallCoinsResult](#massa-abi-v1-GetCallCoinsResult) |  |  |
| get_call_stack_result | [GetCallStackResult](#massa-abi-v1-GetCallStackResult) |  |  |
| get_current_slot_result | [GetCurrentSlotResult](#massa-abi-v1-GetCurrentSlotResult) |  |  |
| get_ds_keys_result | [GetDsKeysResult](#massa-abi-v1-GetDsKeysResult) |  |  |
| get_ds_value_result | [GetDsValueResult](#massa-abi-v1-GetDsValueResult) |  |  |
| get_native_time_result | [GetNativeTimeResult](#massa-abi-v1-GetNativeTimeResult) |  |  |
| get_op_data_result | [GetOpDataResult](#massa-abi-v1-GetOpDataResult) |  |  |
| get_op_keys_result | [GetOpKeysResult](#massa-abi-v1-GetOpKeysResult) |  |  |
| get_origin_operation_id_result | [GetOriginOperationIdResult](#massa-abi-v1-GetOriginOperationIdResult) |  |  |
| get_owned_addresses_result | [GetOwnedAddressesResult](#massa-abi-v1-GetOwnedAddressesResult) |  |  |
| get_pub_key_version_result | [GetPubKeyVersionResult](#massa-abi-v1-GetPubKeyVersionResult) |  |  |
| get_remaining_gas_result | [GetRemainingGasResult](#massa-abi-v1-GetRemainingGasResult) |  |  |
| get_signature_version_result | [GetSignatureVersionResult](#massa-abi-v1-GetSignatureVersionResult) |  |  |
| hash_blake3_result | [HashBlake3Result](#massa-abi-v1-HashBlake3Result) |  |  |
| hash_sha256_result | [HashSha256Result](#massa-abi-v1-HashSha256Result) |  |  |
| op_entry_exists_result | [OpEntryExistsResult](#massa-abi-v1-OpEntryExistsResult) |  |  |
| keccak256_result | [Keccak256Result](#massa-abi-v1-Keccak256Result) |  |  |
| local_execution_response | [LocalExecutionResponse](#massa-abi-v1-LocalExecutionResponse) |  |  |
| native_amount_from_string_result | [NativeAmountFromStringResult](#massa-abi-v1-NativeAmountFromStringResult) |  |  |
| native_amount_to_string_result | [NativeAmountToStringResult](#massa-abi-v1-NativeAmountToStringResult) |  |  |
| scalar_div_rem_native_amount_result | [ScalarDivRemNativeAmountResult](#massa-abi-v1-ScalarDivRemNativeAmountResult) |  |  |
| scalar_mul_native_amount_result | [ScalarMulNativeAmountResult](#massa-abi-v1-ScalarMulNativeAmountResult) |  |  |
| send_async_message_result | [SendAsyncMessageResult](#massa-abi-v1-SendAsyncMessageResult) |  |  |
| set_bytecode_result | [SetBytecodeResult](#massa-abi-v1-SetBytecodeResult) |  |  |
| set_ds_value_result | [SetDsValueResult](#massa-abi-v1-SetDsValueResult) |  |  |
| sub_native_amount_result | [SubNativeAmountResult](#massa-abi-v1-SubNativeAmountResult) |  |  |
| transfer_coins_result | [TransferCoinsResult](#massa-abi-v1-TransferCoinsResult) |  |  |
| unsafe_random_result | [UnsafeRandomResult](#massa-abi-v1-UnsafeRandomResult) |  |  |
| evm_verify_sig_result | [EvmVerifySigResult](#massa-abi-v1-EvmVerifySigResult) |  |  |
| verify_sig_result | [VerifySigResult](#massa-abi-v1-VerifySigResult) |  |  |
| evm_get_address_from_pubkey_result | [EvmGetAddressFromPubkeyResult](#massa-abi-v1-EvmGetAddressFromPubkeyResult) |  |  |
| evm_get_pubkey_from_signature_result | [EvmGetPubkeyFromSignatureResult](#massa-abi-v1-EvmGetPubkeyFromSignatureResult) |  |  |
| is_address_eoa_result | [IsAddressEoaResult](#massa-abi-v1-IsAddressEoaResult) |  |  |
| chain_id_result | [ChainIdResult](#massa-abi-v1-ChainIdResult) |  |  |
| deferred_call_quote_result | [DeferredCallQuoteResult](#massa-abi-v1-DeferredCallQuoteResult) |  |  |
| deferred_call_exists_result | [DeferredCallExistsResult](#massa-abi-v1-DeferredCallExistsResult) |  |  |
| deferred_call_register_result | [DeferredCallRegisterResult](#massa-abi-v1-DeferredCallRegisterResult) |  |  |
| deferred_call_cancel_result | [DeferredCallCancelResult](#massa-abi-v1-DeferredCallCancelResult) |  |  |






<a name="massa-abi-v1-ScalarDivRemNativeAmountRequest"></a>

### ScalarDivRemNativeAmountRequest
Amount division by scalar request
Try to compute quotient = dividend / divisor
Fails if divisor == 0
Fails if underflow


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| dividend | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | Amount to divide |
| divisor | [uint64](#uint64) |  | Divisor to divide by |






<a name="massa-abi-v1-ScalarDivRemNativeAmountResult"></a>

### ScalarDivRemNativeAmountResult
Amount division by scalar result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| quotient | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | Quotient of amount and divisor |
| remainder | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | Remainder of amount and divisor |






<a name="massa-abi-v1-ScalarMulNativeAmountRequest"></a>

### ScalarMulNativeAmountRequest
Amount multiplication request
Try to compute product = amount * coefficient (fail if overflow)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| amount | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | Amount to multiply |
| coefficient | [uint64](#uint64) |  | Coefficient to multiply by |






<a name="massa-abi-v1-ScalarMulNativeAmountResult"></a>

### ScalarMulNativeAmountResult
Amount multiplication result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| product | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | Product of amount and coefficient |






<a name="massa-abi-v1-SendAsyncMessageFilter"></a>

### SendAsyncMessageFilter
Send async message request filter


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| target_address | [string](#string) |  | Target address |
| target_key | [google.protobuf.BytesValue](#google-protobuf-BytesValue) |  | Target key |






<a name="massa-abi-v1-SendAsyncMessageRequest"></a>

### SendAsyncMessageRequest
Send async message request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| target_address | [string](#string) |  | Target address |
| target_handler | [string](#string) |  | Target handler (function name) |
| validity_start | [massa.model.v1.Slot](#massa-model-v1-Slot) |  | Start slot for the message execution |
| validity_end | [massa.model.v1.Slot](#massa-model-v1-Slot) |  | End slot for the message execution |
| execution_gas | [uint64](#uint64) |  | Gas given for the execution |
| raw_fee | [uint64](#uint64) |  | Message fee |
| raw_coins | [uint64](#uint64) |  | Coins sent to the execution context |
| data | [bytes](#bytes) |  | Message data |
| filter | [SendAsyncMessageFilter](#massa-abi-v1-SendAsyncMessageFilter) |  | Filter for the message |






<a name="massa-abi-v1-SendAsyncMessageResult"></a>

### SendAsyncMessageResult
Send async message result






<a name="massa-abi-v1-SetBytecodeRequest"></a>

### SetBytecodeRequest
Set bytecode request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| bytecode | [bytes](#bytes) |  | Bytecode |
| address | [google.protobuf.StringValue](#google-protobuf-StringValue) | optional | Address to set bytecode for, if none, use current address |






<a name="massa-abi-v1-SetBytecodeResult"></a>

### SetBytecodeResult
Set bytecode result






<a name="massa-abi-v1-SetDsValueRequest"></a>

### SetDsValueRequest
Set data store value request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [bytes](#bytes) |  | key |
| value | [bytes](#bytes) |  | value |
| address | [google.protobuf.StringValue](#google-protobuf-StringValue) | optional | Address to set data for, if none, use current address |






<a name="massa-abi-v1-SetDsValueResult"></a>

### SetDsValueResult
Set data store value result






<a name="massa-abi-v1-SubNativeAmountRequest"></a>

### SubNativeAmountRequest
Amount subtraction request
try to compute difference = left - right
fails if right &gt; left


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| left | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | First amount to subtract from |
| right | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | Second amount to subtract |






<a name="massa-abi-v1-SubNativeAmountResult"></a>

### SubNativeAmountResult
Amount subtraction result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| difference | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | Difference of amounts (left - right) |






<a name="massa-abi-v1-TransferCoinsRequest"></a>

### TransferCoinsRequest
Transfer coins request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| target_address | [string](#string) |  | The address of the recipient |
| amount_to_transfer | [massa.model.v1.NativeAmount](#massa-model-v1-NativeAmount) |  | The amount of coins to transfer |
| sender_address | [google.protobuf.StringValue](#google-protobuf-StringValue) | optional | The address of the sender, if none, use current address |






<a name="massa-abi-v1-TransferCoinsResult"></a>

### TransferCoinsResult
Transfer coin result






<a name="massa-abi-v1-UnsafeRandomRequest"></a>

### UnsafeRandomRequest
Unsafe random request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| num_bytes | [uint32](#uint32) |  |  |






<a name="massa-abi-v1-UnsafeRandomResult"></a>

### UnsafeRandomResult
Unsafe random result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| random_bytes | [bytes](#bytes) |  | Random bytes generated |






<a name="massa-abi-v1-VerifySigRequest"></a>

### VerifySigRequest
Verify Sig request


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| sig | [string](#string) |  | Signature to verify |
| message | [bytes](#bytes) |  | Message to verify |
| pub_key | [string](#string) |  | Public key to verify with |






<a name="massa-abi-v1-VerifySigResult"></a>

### VerifySigResult
Verify Sig result


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| is_verified | [bool](#bool) |  | Verification result |





 

 

 

 



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

