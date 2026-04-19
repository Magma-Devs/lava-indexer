// Package relay_payment implements the Handler for Lava's `lava_relay_payment`
// event. One event produces one row per provider entry (the event carries
// multiple providers via indexed attribute keys `provider.0`, `provider.1`, …).
package relay_payment

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/magma-devs/lava-indexer/internal/events"
)

const EventType = "lava_relay_payment"

// Handler persists lava_relay_payment events into app.relay_payments, using
// two dictionary tables (providers, chains) to compress the high-cardinality
// TEXT columns down to 4-byte INT references.
type Handler struct {
	schema    string
	providers *events.Dict
	chains    *events.Dict
}

func New(schema string) *Handler {
	return &Handler{
		schema:    schema,
		providers: events.NewDict(schema, "providers", "addr"),
		chains:    events.NewDict(schema, "chains", "name"),
	}
}

func (h *Handler) Name() string         { return "lava_relay_payment" }
func (h *Handler) EventTypes() []string { return []string{EventType} }

// Warmup pre-loads the providers and chains dictionary caches from
// the DB so steady-state IDs lookups skip the round-trip. Implements
// events.Warmer.
func (h *Handler) Warmup(ctx context.Context, pool *pgxpool.Pool) error {
	if err := h.providers.Warmup(ctx, pool); err != nil {
		return err
	}
	return h.chains.Warmup(ctx, pool)
}

func (h *Handler) DDL() []string {
	return []string{
		h.providers.DDL(),
		h.chains.DDL(),
		fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %[1]s.relay_payments (
		  block_height   BIGINT       NOT NULL,
		  tx_hash        BYTEA        NOT NULL,
		  event_idx      INT          NOT NULL,
		  record_idx     SMALLINT     NOT NULL,
		  timestamp      TIMESTAMPTZ  NOT NULL,
		  provider_id    INT          NOT NULL REFERENCES %[1]s.providers(id),
		  chain_id       INT          NOT NULL REFERENCES %[1]s.chains(id),
		  cu             BIGINT       NOT NULL,
		  relay_number   INT          NOT NULL,
		  qos_avail      REAL,
		  qos_latency    REAL,
		  qos_sync       REAL,
		  ex_qos_avail   REAL,
		  ex_qos_latency REAL,
		  ex_qos_sync    REAL,
		  PRIMARY KEY (block_height, tx_hash, event_idx, record_idx)
		);`, h.schema),
		fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS idx_relay_payments_ts_brin
		  ON %[1]s.relay_payments USING brin (block_height, timestamp)
		  WITH (pages_per_range = 32);`, h.schema),
	}
}

// row mirrors the column order used by COPY below. Keep them in lock-step.
type row struct {
	BlockHeight  int64
	TxHash       []byte
	EventIdx     int32
	RecordIdx    int16
	Timestamp    any
	ProviderID   int32
	ChainID      int32
	CU           int64
	RelayNumber  int32
	QosAvail     *float32
	QosLatency   *float32
	QosSync      *float32
	ExQosAvail   *float32
	ExQosLatency *float32
	ExQosSync    *float32
}

var copyCols = []string{
	"block_height", "tx_hash", "event_idx", "record_idx", "timestamp",
	"provider_id", "chain_id", "cu", "relay_number",
	"qos_avail", "qos_latency", "qos_sync",
	"ex_qos_avail", "ex_qos_latency", "ex_qos_sync",
}

func (h *Handler) Persist(ctx context.Context, tx pgx.Tx, es []events.HandledEvent) error {
	if len(es) == 0 {
		return nil
	}

	// Pre-pass: parse every event into intermediate rows (strings for FK
	// columns) so we know which provider/chain values we need to resolve.
	type preRow struct {
		row
		providerAddr string
		chainName    string
	}
	pre := make([]preRow, 0, len(es)*2)
	uniqProv := make(map[string]struct{})
	uniqChain := make(map[string]struct{})
	for _, he := range es {
		idxs := providerIndices(he.Event.Attrs)
		for _, i := range idxs {
			p := he.Event.Attrs["provider."+strconv.Itoa(i)]
			c := he.Event.Attrs["chainID."+strconv.Itoa(i)]
			cuStr := he.Event.Attrs["CU."+strconv.Itoa(i)]
			if p == "" || c == "" || cuStr == "" {
				continue
			}
			cu, ok := parseInt64(cuStr)
			if !ok {
				continue
			}
			relays := int32(parseInt64OrZero(he.Event.Attrs["relayNumber."+strconv.Itoa(i)]))

			hashBytes, err := decodeTxHash(he.Event.TxHash, he.Block.Height)
			if err != nil {
				return fmt.Errorf("tx_hash decode h=%d: %w", he.Block.Height, err)
			}

			pre = append(pre, preRow{
				row: row{
					BlockHeight:  he.Block.Height,
					TxHash:       hashBytes,
					EventIdx:     int32(he.Event.EventIdx),
					RecordIdx:    int16(i),
					Timestamp:    he.Block.Time,
					CU:           cu,
					RelayNumber:  relays,
					QosAvail:     parseFrac32(he.Event.Attrs["QoSAvailability."+strconv.Itoa(i)]),
					QosLatency:   parseFrac32(he.Event.Attrs["QoSLatency."+strconv.Itoa(i)]),
					QosSync:      parseFrac32(he.Event.Attrs["QoSSync."+strconv.Itoa(i)]),
					ExQosAvail:   parseFrac32(he.Event.Attrs["ExcellenceQoSAvailability."+strconv.Itoa(i)]),
					ExQosLatency: parseFrac32(he.Event.Attrs["ExcellenceQoSLatency."+strconv.Itoa(i)]),
					ExQosSync:    parseFrac32(he.Event.Attrs["ExcellenceQoSSync."+strconv.Itoa(i)]),
				},
				providerAddr: p,
				chainName:    c,
			})
			uniqProv[p] = struct{}{}
			uniqChain[c] = struct{}{}
		}
	}
	if len(pre) == 0 {
		return nil
	}

	provList := make([]string, 0, len(uniqProv))
	for k := range uniqProv {
		provList = append(provList, k)
	}
	chainList := make([]string, 0, len(uniqChain))
	for k := range uniqChain {
		chainList = append(chainList, k)
	}

	provIDs, err := h.providers.IDs(ctx, tx, provList)
	if err != nil {
		return err
	}
	chainIDs, err := h.chains.IDs(ctx, tx, chainList)
	if err != nil {
		return err
	}

	// Build final rows for CopyFrom.
	final := make([][]any, len(pre))
	for i, p := range pre {
		p.row.ProviderID = provIDs[p.providerAddr]
		p.row.ChainID = chainIDs[p.chainName]
		final[i] = []any{
			p.row.BlockHeight, p.row.TxHash, p.row.EventIdx, p.row.RecordIdx, p.row.Timestamp,
			p.row.ProviderID, p.row.ChainID, p.row.CU, p.row.RelayNumber,
			p.row.QosAvail, p.row.QosLatency, p.row.QosSync,
			p.row.ExQosAvail, p.row.ExQosLatency, p.row.ExQosSync,
		}
	}

	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{h.schema, "relay_payments"},
		copyCols,
		pgx.CopyFromRows(final),
	)
	if err != nil {
		return fmt.Errorf("COPY relay_payments: %w", err)
	}
	return nil
}

// providerIndices returns the sorted set of indices `N` for which the event
// carries a `provider.N` attribute.
func providerIndices(attrs map[string]string) []int {
	seen := make(map[int]struct{})
	for k := range attrs {
		if !strings.HasPrefix(k, "provider.") {
			continue
		}
		n, err := strconv.Atoi(k[len("provider."):])
		if err != nil {
			continue
		}
		seen[n] = struct{}{}
	}
	out := make([]int, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

func parseInt64(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		// Big ints — try big.Int and truncate if it fits.
		bi := new(big.Int)
		if _, ok := bi.SetString(s, 10); !ok {
			return 0, false
		}
		if !bi.IsInt64() {
			return 0, false
		}
		return bi.Int64(), true
	}
	return n, true
}

func parseInt64OrZero(s string) int64 {
	v, _ := parseInt64(s)
	return v
}

func parseFrac32(s string) *float32 {
	if s == "" {
		return nil
	}
	if i := strings.IndexByte(s, '/'); i >= 0 {
		num, err1 := strconv.ParseFloat(s[:i], 64)
		den, err2 := strconv.ParseFloat(s[i+1:], 64)
		if err1 != nil || err2 != nil || den == 0 {
			return nil
		}
		v := float32(num / den)
		return &v
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	f := float32(v)
	return &f
}

// decodeTxHash converts the hex string from rpc.Event to bytea. For
// synthetic non-tx events (TxHash empty), we synthesize a 32-byte sentinel
// that's unique per block so the composite PK stays unique.
func decodeTxHash(s string, height int64) ([]byte, error) {
	if s == "" {
		// 32-byte sentinel: 0xff repeated with height encoded in the last 8 bytes
		out := make([]byte, 32)
		for i := 0; i < 24; i++ {
			out[i] = 0xff
		}
		h := uint64(height)
		for i := 0; i < 8; i++ {
			out[31-i] = byte(h >> (8 * i))
		}
		return out, nil
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, err
	}
	return b, nil
}
