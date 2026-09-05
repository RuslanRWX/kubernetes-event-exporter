package sinks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFormatIndexName(t *testing.T) {
	when := time.Date(2026, 9, 5, 13, 45, 8, 0, time.UTC)

	tests := []struct {
		name     string
		pattern  string
		expected string
	}{
		{"no pattern", "kube-events", "kube-events"},
		{"daily dotted", "events-k8s2-{2006.01.02}", "events-k8s2-2026.09.05"},
		{"daily dashed", "kube-events-{2006-01-02}", "kube-events-2026-09-05"},
		{"leading token", "{2006}-kube-events", "2026-kube-events"},
		{"hourly", "kube-events-{2006-01-02-15}", "kube-events-2026-09-05-13"},
		// A greedy `{(.*)}` collapses everything between the first and last brace
		// into a single time layout, producing garbage for multi-token patterns.
		{"two tokens", "events-{2006}-{01.02}", "events-2026-09.05"},
		{"token between literals", "a{2006}b{01}c", "a2026b09c"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, formatIndexName(tc.pattern, when))
			assert.Equal(t, tc.expected, osFormatIndexName(tc.pattern, when))
		})
	}
}
