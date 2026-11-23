package chains

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/tidwall/gjson"
)

// Rules operate on both replies so a field supplied by only one provider
// cannot enter the verified response. nil means omit, RawMessage("null")
// preserves an explicit protocol null (e.g. Solana meta.err on success).
type evidenceRule func(a, b gjson.Result) (any, error)

var ErrTransactionPending = errors.New("transaction is pending confirmation")
var ErrEvidenceConflict = errors.New("transaction evidence differs")

type evidenceFields map[string]evidenceRule

func (fields evidenceFields) match(a, b gjson.Result) (any, error) {
	if !a.IsObject() || !b.IsObject() {
		return nil, ErrTransactionData
	}
	out := make(map[string]any, len(fields))
	for _, key := range slices.Sorted(maps.Keys(fields)) {
		v, err := fields[key](a.Get(key), b.Get(key))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		if v != nil {
			out[key] = v
		}
	}
	return out, nil
}

// Input comes from canonicalTransaction, which sorts object keys and keeps
// integers exact. Move values are defined by on-chain application types.
func equalValue(a, b gjson.Result) (any, error) {
	if !a.Exists() || !b.Exists() {
		return nil, ErrTransactionData
	}
	if a.Raw != b.Raw {
		return nil, ErrEvidenceConflict
	}
	return json.RawMessage(a.Raw), nil
}

func textValue(a, b gjson.Result) (any, error) {
	if a.Type != gjson.String || b.Type != gjson.String || a.Str == "" || b.Str == "" {
		return nil, ErrTransactionData
	}
	return equalValue(a, b)
}

// A compiled Solana instruction can have empty instruction data.
func stringValue(a, b gjson.Result) (any, error) {
	if a.Type != gjson.String || b.Type != gjson.String {
		return nil, ErrTransactionData
	}
	return equalValue(a, b)
}

func boolValue(a, b gjson.Result) (any, error) {
	if (a.Type != gjson.True && a.Type != gjson.False) || (b.Type != gjson.True && b.Type != gjson.False) {
		return nil, ErrTransactionData
	}
	return equalValue(a, b)
}

func optional(rule evidenceRule) evidenceRule {
	return func(a, b gjson.Result) (any, error) {
		if a.Type == gjson.Null || b.Type == gjson.Null {
			return nil, nil
		}
		return rule(a, b)
	}
}

func list(rule evidenceRule) evidenceRule {
	return func(a, b gjson.Result) (any, error) {
		if !a.IsArray() || !b.IsArray() {
			return nil, ErrTransactionData
		}
		left, right := a.Array(), b.Array()
		if len(left) != len(right) {
			return nil, ErrEvidenceConflict
		}
		out := make([]any, len(left))
		for i := range left {
			v, err := rule(left[i], right[i])
			if err != nil {
				return nil, fmt.Errorf("[%d]: %w", i, err)
			}
			out[i] = v
		}
		return out, nil
	}
}

// Missing historic metadata cannot vouch for a transfer reported by its peer.
func optionalList(rule evidenceRule) evidenceRule {
	match := list(rule)
	return func(a, b gjson.Result) (any, error) {
		if a.Type == gjson.Null || b.Type == gjson.Null {
			if len(a.Array()) > 0 || len(b.Array()) > 0 {
				return nil, ErrTransactionData
			}
			return nil, nil
		}
		return match(a, b)
	}
}

func loadedAddresses(a, b gjson.Result) (any, error) {
	if a.Type == gjson.Null || b.Type == gjson.Null {
		for _, v := range []gjson.Result{a, b} {
			if len(v.Get("writable").Array())+len(v.Get("readonly").Array()) > 0 {
				return nil, ErrTransactionData
			}
		}
		return nil, nil
	}
	return evidenceFields{"writable": list(textValue), "readonly": list(textValue)}.match(a, b)
}

// Protobuf omits empty repeated fields. Never discard a nonempty list when
// the other provider omits it.
func repeated(rule evidenceRule) evidenceRule {
	match := list(rule)
	return func(a, b gjson.Result) (any, error) {
		if !a.Exists() {
			a = gjson.Parse("[]")
		}
		if !b.Exists() {
			b = gjson.Parse("[]")
		}
		return match(a, b)
	}
}

func solExecutionError(a, b gjson.Result) (any, error) {
	for _, v := range []gjson.Result{a, b} {
		// Solana serializes TransactionError as an enum or tagged object.
		if v.Raw != "null" && v.Type != gjson.String && !v.IsObject() {
			return nil, ErrTransactionData
		}
	}
	return equalValue(a, b)
}

var solInstruction = evidenceFields{
	"programIdIndex": uintValue, "accounts": list(uintValue), "data": stringValue,
}

var solBalance = evidenceFields{
	"accountIndex": uintValue, "mint": textValue, "owner": optional(textValue),
	"uiTokenAmount": evidenceFields{"amount": decimalText, "decimals": uintValue}.match,
}

var solEvidence = evidenceFields{
	"slot": uintValue, "blockTime": optional(uintValue),
	"transaction": evidenceFields{
		"signatures": list(textValue),
		"message": evidenceFields{
			"accountKeys": list(textValue), "instructions": list(solInstruction.match),
			"recentBlockhash": textValue,
		}.match,
	}.match,
	"meta": evidenceFields{
		"err":               solExecutionError,
		"innerInstructions": optionalList(evidenceFields{"index": uintValue, "instructions": list(solInstruction.match)}.match),
		"loadedAddresses":   loadedAddresses,
		"postTokenBalances": optionalList(solBalance.match),
	}.match,
}

var aptosEvidence = evidenceFields{
	"hash": textValue, "type": textValue, "version": decimalText, "timestamp": decimalText, "success": boolValue,
	"events":  optionalList(evidenceFields{"type": textValue, "data": equalValue}.match),
	"changes": list(aptosChange),
}

func aptosChange(a, b gjson.Result) (any, error) {
	fields := evidenceFields{"type": textValue, "state_key_hash": textValue}
	switch a.Get("type").Str {
	case "write_resource":
		fields["address"] = textValue
		fields["data"] = evidenceFields{"type": textValue, "data": equalValue}.match
	case "write_table_item":
		fields["handle"], fields["key"], fields["value"] = textValue, textValue, textValue
	}
	return fields.match(a, b)
}

func digits(s string) bool {
	return s != "" && strings.Trim(s, "0123456789") == ""
}

func uintValue(a, b gjson.Result) (any, error) {
	if a.Type != gjson.Number || b.Type != gjson.Number || !digits(a.Raw) || !digits(b.Raw) {
		return nil, ErrTransactionData
	}
	return equalValue(a, b)
}

func decimalText(a, b gjson.Result) (any, error) {
	if !digits(a.Str) || !digits(b.Str) {
		return nil, ErrTransactionData
	}
	return textValue(a, b)
}

var suiEvidence = evidenceFields{
	"digest": textValue, "checkpoint": decimalText, "timestamp": optional(textValue),
	"transaction":     evidenceFields{"sender": textValue}.match,
	"effects":         evidenceFields{"status": evidenceFields{"success": boolValue}.match}.match,
	"balance_changes": repeated(evidenceFields{"address": textValue, "coin_type": textValue, "amount": signedDecimal}.match),
}

func signedDecimal(a, b gjson.Result) (any, error) {
	if !digits(strings.TrimPrefix(a.Str, "-")) || !digits(strings.TrimPrefix(b.Str, "-")) {
		return nil, ErrTransactionData
	}
	return textValue(a, b)
}

// An omitted TransactionInfo.result is the protobuf default SUCESS. A
// missing receipt.result is normal on native transfers. A reported failure
// must never disappear through optional-field handling.
func tronResult(a, b gjson.Result) (any, error) {
	for _, v := range []gjson.Result{a, b} {
		if v.Exists() && v.Str != "SUCCESS" && v.Str != "SUCESS" {
			return textValue(a, b)
		}
	}
	return optional(textValue)(a, b)
}

// The wallet endpoints use hex with visible=false. Compare the bytes while
// preserving their length (leading zero bytes in log data are significant).
func tronHexBytes(a, b gjson.Result) (any, error) {
	if a.Type != gjson.String || b.Type != gjson.String {
		return nil, ErrTransactionData
	}
	if _, err := hex.DecodeString(a.Str); err != nil {
		return nil, ErrTransactionData
	}
	if _, err := hex.DecodeString(b.Str); err != nil {
		return nil, ErrTransactionData
	}
	if !strings.EqualFold(a.Str, b.Str) {
		return nil, ErrEvidenceConflict
	}
	return strings.ToLower(a.Str), nil
}

var tronEvidence = evidenceFields{
	"transaction": evidenceFields{
		"txID": tronHexBytes, "raw_data_hex": optional(tronHexBytes), "ret": list(evidenceFields{"contractRet": textValue}.match),
		"raw_data": evidenceFields{
			"contract": list(tronContract),
		}.match,
	}.match,
	"receipt": evidenceFields{
		"id": tronHexBytes, "blockNumber": uintValue, "blockTimeStamp": uintValue, "result": tronResult,
		"receipt": tronReceipt,
		"log":     repeated(evidenceFields{"address": tronHexBytes, "topics": repeated(tronHexBytes), "data": tronLogData}.match),
	}.match,
}

func tronContract(a, b gjson.Result) (any, error) {
	fields := evidenceFields{"type": textValue}
	var value evidenceFields
	switch a.Get("type").Str {
	case "TransferContract":
		value = evidenceFields{"owner_address": tronHexBytes, "to_address": tronHexBytes, "amount": uintValue}
	case "TriggerSmartContract":
		value = evidenceFields{"owner_address": tronHexBytes, "contract_address": tronHexBytes, "data": tronLogData,
			"call_value": tronCallValue, "call_token_value": tronCallValue, "token_id": tronCallValue}
	}
	if value != nil {
		fields["parameter"] = evidenceFields{"value": value.match}.match
	}
	return fields.match(a, b)
}

func tronCallValue(a, b gjson.Result) (any, error) {
	if !a.Exists() || !b.Exists() {
		if (a.Exists() && a.Raw != "0") || (b.Exists() && b.Raw != "0") {
			return nil, ErrTransactionData
		}
		return nil, nil
	}
	return uintValue(a, b)
}

func tronLogData(a, b gjson.Result) (any, error) {
	// bytes fields are omitted when empty in the protobuf JSON response.
	for _, v := range []*gjson.Result{&a, &b} {
		if !v.Exists() {
			*v = gjson.Parse(`""`)
		}
	}
	return tronHexBytes(a, b)
}

func tronReceipt(a, b gjson.Result) (any, error) {
	if (a.Exists() && !a.IsObject()) || (b.Exists() && !b.IsObject()) {
		return nil, ErrTransactionData
	}
	v, err := tronResult(a.Get("result"), b.Get("result"))
	if err != nil || v == nil {
		return nil, err
	}
	return map[string]any{"result": v}, nil
}

// Use raw message contents, not the indexer's optional ABI decoding.
var tonMessage = evidenceFields{
	"hash": tonHash, "source": optional(tonAccount), "destination": optional(tonAccount),
	"value": optional(decimalText), "bounced": adverseFlag, "bounce": optional(boolValue),
	"message_content": optional(evidenceFields{"hash": tonHash, "body": textValue}.match),
}

func adverseFlag(a, b gjson.Result) (any, error) {
	if a.Type == gjson.True || b.Type == gjson.True {
		return boolValue(a, b)
	}
	return optional(boolValue)(a, b)
}

func nullableMessage(a, b gjson.Result) (any, error) {
	if a.Type == gjson.Null && b.Type == gjson.Null && (!a.Exists() || !b.Exists()) {
		return nil, nil
	}
	if a.Type == gjson.Null || b.Type == gjson.Null {
		return equalValue(a, b)
	}
	return tonMessage.match(a, b)
}

// Index votes bind the consumer's transaction cell to a confirmed chain
// location. Only direct LiteServer votes may return a proven transaction BOC.
var tonIndexEvidence = evidenceFields{
	"hash": tonHash, "account": tonAccount, "lt": decimalText, "now": uintValue, "mc_block_seqno": uintValue,
	"block_ref":   evidenceFields{"workchain": equalValue, "shard": textValue, "seqno": uintValue}.match,
	"description": optional(evidenceFields{"type": textValue, "aborted": adverseFlag, "destroyed": adverseFlag}.match),
	"in_msg":      nullableMessage, "out_msgs": optionalList(tonMessage.match),
}

func tonHash(a, b gjson.Result) (any, error) {
	left, err := TransactionHash("ton", a.Str)
	if err != nil {
		return nil, err
	}
	right, err := TransactionHash("ton", b.Str)
	if err != nil {
		return nil, err
	}
	if left != right {
		return nil, ErrEvidenceConflict
	}
	raw, _ := hex.DecodeString(left)
	return base64.StdEncoding.EncodeToString(raw), nil
}

func tonAccount(a, b gjson.Result) (any, error) {
	left, err := Canonical("ton", a.Str)
	if err != nil {
		return nil, err
	}
	right, err := Canonical("ton", b.Str)
	if err != nil {
		return nil, err
	}
	if left != right {
		return nil, ErrEvidenceConflict
	}
	return left, nil
}

func checkStrictRecord(n Network, v gjson.Result) error {
	switch {
	case IsEVM(n.Name):
		if v.Get("receipt").Raw == "null" {
			return ErrTransactionPending
		}
	case n.Name == "aptos":
		switch v.Get("type").Str {
		case "pending_transaction":
			return ErrTransactionPending
		case "user_transaction":
			if !v.Get("events").IsArray() {
				return ErrTransactionData
			}
		}
	case n.Name == "tron":
		if r := v.Get("receipt"); r.IsObject() && len(r.Map()) == 0 {
			return ErrTransactionPending
		}
		count := len(v.Get("transaction.raw_data.contract").Array())
		if count == 0 || len(v.Get("transaction.ret").Array()) != count {
			return ErrTransactionData
		}
	case n.Name == "ton" && n.Protocol == "toncenter-v3":
		if flag := v.Get("emulated"); flag.Exists() && flag.Type != gjson.False {
			return ErrTransactionData
		}
		if finality := v.Get("finality"); finality.Exists() && finality.Str != "finalized" {
			return ErrTransactionPending
		}
	}
	return nil
}

// AgreeTransaction receives two successful, identity-checked lookup results.
func AgreeTransaction(n Network, left, right json.RawMessage) (json.RawMessage, error) {
	a, b := gjson.ParseBytes(left), gjson.ParseBytes(right)
	for _, v := range []gjson.Result{a, b} {
		if err := checkStrictRecord(n, v); err != nil {
			return nil, err
		}
	}

	if IsEVM(n.Name) || n.Name == "ton" && n.Protocol == "lite" {
		// These readers already return locally constructed canonical evidence.
		if len(left) == 0 || !bytes.Equal(left, right) {
			return nil, ErrEvidenceConflict
		}
		return left, nil
	}
	return projectTransaction(n, a, b)
}

// FallbackTransactionEvidence validates and projects a single successful,
// identity-checked read from an operator-configured fallback.
func FallbackTransactionEvidence(n Network, raw json.RawMessage) (json.RawMessage, error) {
	v := gjson.ParseBytes(raw)
	if !v.IsObject() {
		return nil, ErrTransactionData
	}
	if err := checkStrictRecord(n, v); err != nil {
		return nil, err
	}
	if IsEVM(n.Name) {
		return canonicalEVMEvidence(v.Get("transaction"), v.Get("receipt"))
	}
	if n.Name == "ton" && n.Protocol == "lite" {
		return raw, nil
	} // VerifyTON has checked the inclusion proof.
	// Apply the same field and type rules to this record without claiming a second vote.
	return projectTransaction(n, v, v)
}

func projectTransaction(n Network, a, b gjson.Result) (json.RawMessage, error) {
	var fields evidenceFields
	switch {
	case n.Name == "solana":
		fields = solEvidence
	case n.Name == "aptos":
		fields = aptosEvidence
	case n.Name == "sui":
		fields = suiEvidence
	case n.Name == "tron":
		fields = tronEvidence
	case n.Name == "ton" && n.Protocol == "toncenter-v3":
		fields = tonIndexEvidence
	default:
		return nil, ErrTransactionData
	}
	v, err := fields.match(a, b)
	if err != nil {
		return nil, err
	}
	return json.Marshal(v)
}
