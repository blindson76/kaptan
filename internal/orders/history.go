package orders

import (
	"context"
	"strings"

	"github.com/umitbozkurt/consul-replctl/internal/store"
)

// SaveWithHistory writes the order to the main key and keeps up to `keep` past entries
// under a derived history key (orders_history/...).
func SaveWithHistory(ctx context.Context, kv store.KV, orderKey string, ord Order, keep int) error {
	if err := kv.PutJSON(ctx, orderKey, &ord); err != nil {
		return err
	}
	if keep <= 0 {
		return nil
	}

	histKey := historyKey(orderKey)
	var hist []Order
	_, _ = kv.GetJSON(ctx, histKey, &hist)
	hist = append(hist, ord)
	if keep > 0 && len(hist) > keep {
		hist = hist[len(hist)-keep:]
	}
	return kv.PutJSON(ctx, histKey, &hist)
}

func historyKey(orderKey string) string {
	trimmed := strings.TrimPrefix(orderKey, "/")
	if strings.HasPrefix(trimmed, "orders/") {
		return "orders_history/" + strings.TrimPrefix(trimmed, "orders/")
	}
	return "orders_history/" + trimmed
}
