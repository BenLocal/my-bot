package core

import (
	"fmt"
	"sort"
	"time"

	"gopkg.in/yaml.v3"
)

// RolesConfig is config/roles.yaml (spec §4.1).
type RolesConfig struct {
	Roles        map[string]RoleConfig `yaml:"roles"`
	DefaultBrain string                `yaml:"defaultBrain"`
	Limits       ConfigLimits          `yaml:"limits"`
}

// RoleConfig is one role entry in roles.yaml.
type RoleConfig struct {
	Executor     string   `yaml:"executor"`
	Account      string   `yaml:"account"`
	Instructions string   `yaml:"instructions"` // 指令文件路径（相对仓库 / ConfigMap 根）
	Timeout      string   `yaml:"timeout"`
	Capabilities []string `yaml:"capabilities"`
}

// ConfigLimits is the limits block of roles.yaml.
type ConfigLimits struct {
	GlobalConcurrency int `yaml:"globalConcurrency"`
	Goal              struct {
		MaxParallel      int `yaml:"maxParallel"`
		MaxStepsPerRound int `yaml:"maxStepsPerRound"`
	} `yaml:"goal"`
	Accounts map[string]struct {
		Concurrency int `yaml:"concurrency"`
	} `yaml:"accounts"`
}

// ParseRolesConfig parses and validates roles.yaml content.
func ParseRolesConfig(data []byte) (RolesConfig, error) {
	var cfg RolesConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return RolesConfig{}, fmt.Errorf("parse roles config: %w", err)
	}
	if len(cfg.Roles) == 0 {
		return RolesConfig{}, fmt.Errorf("roles config: no roles defined")
	}
	names := make([]string, 0, len(cfg.Roles))
	for name := range cfg.Roles {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		r := cfg.Roles[name]
		if r.Executor == "" || r.Account == "" {
			return RolesConfig{}, fmt.Errorf("roles config: role %q needs executor and account", name)
		}
		if r.Timeout != "" {
			if _, err := time.ParseDuration(r.Timeout); err != nil {
				return RolesConfig{}, fmt.Errorf("roles config: role %q timeout: %w", name, err)
			}
		}
	}
	if cfg.DefaultBrain != "" {
		if _, ok := cfg.Roles[cfg.DefaultBrain]; !ok {
			return RolesConfig{}, fmt.Errorf("roles config: defaultBrain %q is not a role", cfg.DefaultBrain)
		}
	}
	return cfg, nil
}

// AccountLimits returns "executor/account" → concurrency.
func (c RolesConfig) AccountLimits() map[string]int {
	out := make(map[string]int, len(c.Limits.Accounts))
	for k, v := range c.Limits.Accounts {
		out[k] = v.Concurrency
	}
	return out
}

// HasCapability reports whether the role config lists the capability.
func (r RoleConfig) HasCapability(c string) bool {
	for _, x := range r.Capabilities {
		if x == c {
			return true
		}
	}
	return false
}

// TimeoutDuration returns the role timeout, defaulting to 30m.
func (r Role) TimeoutDuration() time.Duration {
	if d, err := time.ParseDuration(r.Timeout); err == nil && d > 0 {
		return d
	}
	return 30 * time.Minute
}
