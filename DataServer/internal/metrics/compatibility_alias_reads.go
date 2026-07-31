package metrics

import "velox-shared/compatibility"

// RecordCompatibilityAliasRead records one legacy alias read. Labels are
// intentionally only alias and canonical; neither may contain job identity.
func (c *Collector) RecordCompatibilityAliasRead(alias, canonical string) {
	if c == nil || c.compatibilityAliasReads == nil {
		return
	}
	c.compatibilityAliasReads.Inc([]string{alias, canonical}, 1)
}

// NewCompatibilityAliasObserver returns an observer suitable for
// compatibility.SetAliasReadObserver.
func (c *Collector) NewCompatibilityAliasObserver() compatibility.AliasReadObserver {
	return func(alias, canonical string) {
		c.RecordCompatibilityAliasRead(alias, canonical)
	}
}
