package proxy

import (
	"reflect"
	"testing"

	"github.com/vfolgosa/bifrost-proxy/internal/config"
)

func TestClusterBootstraps(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.ClusterConfig
		want []string
	}{
		{
			name: "single primary only",
			cfg: config.ClusterConfig{
				Mode:    config.ModeSingle,
				Primary: config.ClusterEndpoint{Bootstrap: "p:9092"},
			},
			want: []string{"p:9092"},
		},
		{
			name: "active_passive both endpoints",
			cfg: config.ClusterConfig{
				Mode:      config.ModeActivePassive,
				Primary:   config.ClusterEndpoint{Bootstrap: "p:9092"},
				Secondary: config.ClusterEndpoint{Bootstrap: "s:9092"},
			},
			want: []string{"p:9092", "s:9092"},
		},
		{
			name: "load_balance both endpoints",
			cfg: config.ClusterConfig{
				Mode:      config.ModeLoadBalance,
				Primary:   config.ClusterEndpoint{Bootstrap: "p:9092"},
				Secondary: config.ClusterEndpoint{Bootstrap: "s:9092"},
			},
			want: []string{"p:9092", "s:9092"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := clusterBootstraps(tt.cfg)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("clusterBootstraps() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAllConfigBootstraps_Deduplicates(t *testing.T) {
	cfg := &config.Config{
		Clusters: map[string]config.ClusterConfig{
			"bu-a": {
				Mode:      config.ModeLoadBalance,
				Primary:   config.ClusterEndpoint{Bootstrap: "shared:9092"},
				Secondary: config.ClusterEndpoint{Bootstrap: "s:9092"},
			},
			"bu-b": {
				Mode:    config.ModeSingle,
				Primary: config.ClusterEndpoint{Bootstrap: "shared:9092"},
			},
		},
	}

	got := allConfigBootstraps(cfg)
	want := []string{"shared:9092", "s:9092"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("allConfigBootstraps() = %v, want %v", got, want)
	}
}
