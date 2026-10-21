package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/BenLocal/my-bot/internal/core"
)

// writeSSEEvent writes a single event to an SSE stream in the format:
//
//	id: <goalID>:<seq>
//	event: <type>
//	data: <json>
//
// A trailing blank line delimits the event. The flusher is used immediately so
// events reach the client without batching delays.
func writeSSEEvent(w http.ResponseWriter, flusher http.Flusher, goalID string, ev core.Event) error {
	data, err := json.Marshal(ev.Data)
	if err != nil {
		return err
	}

	var b strings.Builder
	fmt.Fprintf(&b, "id: %s:%d\n", goalID, ev.Seq)
	fmt.Fprintf(&b, "event: %s\n", ev.Type)
	fmt.Fprintf(&b, "data: %s\n\n", data)

	if _, err := io.WriteString(w, b.String()); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}
