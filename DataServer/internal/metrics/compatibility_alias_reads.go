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
// RecordCompatibilityAliasRejection records one strict-mode alias rejection.
func (c *Collector) RecordCompatibilityAliasRejection(alias, canonical string) {
	if c == nil || c.compatibilityAliasRejections == nil {
		return
	}
	c.compatibilityAliasRejections.Inc([]string{alias, canonical}, 1)
}

// NewCompatibilityAliasRejectionObserver returns the strict-mode observer.
func (c *Collector) NewCompatibilityAliasRejectionObserver() compatibility.AliasRejectedObserver {
	return func(alias, canonical string) {
		c.RecordCompatibilityAliasRejection(alias, canonical)
	}
}

func (c *Collector) NewCompatibilityAliasObserver() compatibility.AliasReadObserver {
	return func(alias, canonical string) {
		c.RecordCompatibilityAliasRead(alias, canonical)
	}
}
