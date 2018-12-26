package config

import (
	"fmt"

	"github.com/hashicorp/hcl"
)

func (c *Config) load(b []byte) error {
	var cfg map[string]interface{}

	err := hcl.Decode(&cfg, string(b))
	if err != nil {
		return err
	}
	for name, val := range cfg {
		cvar, ok := c.vars[name]
		if !ok {
			return fmt.Errorf("%s is not a config variable", name)
		}
		if cvar.noConfig {
			return fmt.Errorf("%s can't be set in config file", name)
		}

		if cvar.by == byDefault {
			err := cvar.val.SetValue(val)
			if err != nil {
				return fmt.Errorf("%s: %s", cvar.name, err)
			}
			cvar.by = byConfig
		}
	}

	return nil
}
