//go:build !functional

package sarama

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type stubProtocolStrategy struct {
	BalanceStrategy
	protocols []RebalanceProtocol
}

func (s *stubProtocolStrategy) SupportedProtocols() []RebalanceProtocol {
	return s.protocols
}

func TestSelectRebalanceProtocol(t *testing.T) {
	eagerOnly := &stubProtocolStrategy{
		BalanceStrategy: NewBalanceStrategyRange(),
		protocols:       []RebalanceProtocol{RebalanceProtocolEager},
	}
	both := &stubProtocolStrategy{
		BalanceStrategy: NewBalanceStrategyRange(),
		protocols:       []RebalanceProtocol{RebalanceProtocolCooperative, RebalanceProtocolEager},
	}
	cooperativeOnly := &stubProtocolStrategy{
		BalanceStrategy: NewBalanceStrategyRange(),
		protocols:       []RebalanceProtocol{RebalanceProtocolCooperative},
	}

	tests := []struct {
		name       string
		strategies []BalanceStrategy
		want       RebalanceProtocol
		wantErr    bool
	}{
		{
			name:       "no strategies has no common protocol",
			strategies: nil,
			wantErr:    true,
		},
		{
			name:       "strategy without declared protocols defaults to eager",
			strategies: []BalanceStrategy{NewBalanceStrategyRange()},
			want:       RebalanceProtocolEager,
		},
		{
			name:       "single cooperative-capable strategy selects cooperative",
			strategies: []BalanceStrategy{both},
			want:       RebalanceProtocolCooperative,
		},
		{
			name:       "cooperative-capable alongside eager-only selects eager",
			strategies: []BalanceStrategy{both, eagerOnly},
			want:       RebalanceProtocolEager,
		},
		{
			name:       "order does not affect selection",
			strategies: []BalanceStrategy{eagerOnly, both},
			want:       RebalanceProtocolEager,
		},
		{
			name:       "highest common protocol wins",
			strategies: []BalanceStrategy{both, cooperativeOnly},
			want:       RebalanceProtocolCooperative,
		},
		{
			name:       "disjoint protocols have no common protocol",
			strategies: []BalanceStrategy{cooperativeOnly, eagerOnly},
			wantErr:    true,
		},
		{
			name: "duplicate protocols within one strategy are not double counted",
			strategies: []BalanceStrategy{
				&stubProtocolStrategy{
					BalanceStrategy: NewBalanceStrategyRange(),
					protocols: []RebalanceProtocol{
						RebalanceProtocolCooperative,
						RebalanceProtocolCooperative,
					},
				},
				eagerOnly,
			},
			wantErr: true,
		},
		{
			name: "unknown protocol is rejected",
			strategies: []BalanceStrategy{&stubProtocolStrategy{
				BalanceStrategy: NewBalanceStrategyRange(),
				protocols:       []RebalanceProtocol{42},
			}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := selectRebalanceProtocol(tt.strategies)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
