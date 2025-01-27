package prom

const (
	// AllocationContextName is the name we assign the allocation query context [metadata]
	AllocationContextName = "allocation"

	// ClusterContextName is the name we assign the cluster query context [metadata]
	ClusterContextName = "cluster"

	// ClusterContextName is the name we assign the optional cluster query context [metadata]
	ClusterOptionalContextName = "cluster-optional"

	// ComputeCostDataContextName is the name we assign the compute cost data query context [metadata]
	ComputeCostDataContextName = "compute-cost-data"

	// ComputeCostDataContextName is the name we assign the compute cost data range query context [metadata]
	ComputeCostDataRangeContextName = "compute-cost-data-range"

	// ClusterMapContextName is the name we assign the cluster map query context [metadata]
	ClusterMapContextName = "cluster-map"

	// FrontendContextName is the name we assign queries proxied from the frontend [metadata]
	FrontendContextName = "frontend"

	// DiagnosticContextName is the name we assign queries that check the state of the prometheus connection
	DiagnosticContextName = "diagnostic"

	// ContainerStatsContextName is the name we assign queries that build
	// container stats aggregations.
	ContainerStatsContextName = "container-stats"
)
