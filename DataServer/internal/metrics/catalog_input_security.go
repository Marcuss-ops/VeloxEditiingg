package metrics

func inputSecurityMetricDefinitions() []MetricDefinition {
	return []MetricDefinition{
		{Name: "input.security_rejections_total", Unit: "count", Component: CompInput, Kind: KindCounter, Description: "Rejected input acquisitions by role and canonical error code"},
		{Name: "input.security_rejected_bytes_total", Unit: "bytes", Component: CompInput, Kind: KindCounter, Description: "Bytes rejected while acquiring inputs"},
		{Name: "input.security_quarantined_total", Unit: "count", Component: CompInput, Kind: KindCounter, Description: "Suspicious input files moved to quarantine"},
	}
}
