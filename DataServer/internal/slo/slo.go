// Package slo contains the small, machine-readable SLO catalog used by
// dashboards and alert rules.
package slo

type Definition struct {
	Name, Metric, Window string
	Target               float64
	Comparator           string
}

var catalog = []Definition{
	{Name: "valid_job_acceptance", Metric: "jobs.acceptance.success_ratio", Window: "30d", Target: .99, Comparator: ">="},
	{Name: "normal_job_start", Metric: "jobs.start_within_2m_ratio", Window: "30d", Target: .95, Comparator: ">="},
	{Name: "task_result_persistence", Metric: "task_results.persisted_ratio", Window: "30d", Target: .999, Comparator: ">="},
	{Name: "publication_without_intervention", Metric: "publications.first_pass_ratio", Window: "30d", Target: .99, Comparator: ">="},
	{Name: "verified_artifact_publication", Metric: "artifacts.published_hash_verified_ratio", Window: "30d", Target: 1, Comparator: ">="},
	{Name: "scheduled_publication_accuracy", Metric: "publications.within_60s_ratio", Window: "30d", Target: .99, Comparator: ">="},
}

func Meets(def Definition, observed float64) bool {
	if def.Comparator == ">=" {
		return observed >= def.Target
	}
	return observed <= def.Target
}
