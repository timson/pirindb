package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type PirinDBStorageSpec struct {
	// +kubebuilder:default="10Gi"
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]*([EPTGMK]i?)?$`
	Size             string  `json:"size,omitempty"`
	StorageClassName *string `json:"storageClassName,omitempty"`
}

type PirinDBClusterSpec struct {
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	Replicas *int32 `json:"replicas,omitempty"`
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`
	// +kubebuilder:default=IfNotPresent
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`
	// +kubebuilder:default=4321
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	HTTPPort int32 `json:"httpPort,omitempty"`
	// +kubebuilder:default=6379
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	RedisPort int32 `json:"redisPort,omitempty"`
	// +kubebuilder:default=16384
	// +kubebuilder:validation:Minimum=16
	// +kubebuilder:validation:Maximum=65535
	SlotCount int32 `json:"slotCount,omitempty"`
	// +kubebuilder:default=INFO
	// +kubebuilder:validation:Enum=INFO;WARNING;DEBUG;ERROR
	LogLevel string `json:"logLevel,omitempty"`
	// +kubebuilder:default=strict
	// +kubebuilder:validation:Enum=strict;journal;group
	SyncPolicy string `json:"syncPolicy,omitempty"`
	// +kubebuilder:default=64
	// +kubebuilder:validation:Minimum=1
	CheckpointTxThreshold int32 `json:"checkpointTxThreshold,omitempty"`
	// +kubebuilder:default=16
	// +kubebuilder:validation:Minimum=1
	GroupCommitTxThreshold int32 `json:"groupCommitTxThreshold,omitempty"`
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	GroupCommitWindowMs int32                       `json:"groupCommitWindowMs,omitempty"`
	Storage             PirinDBStorageSpec          `json:"storage,omitempty"`
	Resources           corev1.ResourceRequirements `json:"resources,omitempty"`
	// +kubebuilder:default=true
	AutoRebalanceOnScaleUp *bool `json:"autoRebalanceOnScaleUp,omitempty"`
	// +kubebuilder:default=true
	AutoRebalanceOnScaleDown *bool `json:"autoRebalanceOnScaleDown,omitempty"`
	// +kubebuilder:validation:Required
	AdminSecretRef *corev1.SecretKeySelector `json:"adminSecretRef"`
}

type PirinDBClusterRuntimeCondition struct {
	Type    string `json:"type,omitempty"`
	Status  string `json:"status,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

type PirinDBClusterRuntimeNodeStatus struct {
	NodeID               string `json:"nodeID,omitempty"`
	RedisAddress         string `json:"redisAddress,omitempty"`
	HTTPAddress          string `json:"httpAddress,omitempty"`
	OwnedSlots           int32  `json:"ownedSlots,omitempty"`
	TargetSlots          int32  `json:"targetSlots,omitempty"`
	ConfiguredInTopology bool   `json:"configuredInTopology,omitempty"`
	Draining             bool   `json:"draining,omitempty"`
	Receiving            bool   `json:"receiving,omitempty"`
}

type PirinDBClusterOperationStatus struct {
	Kind           string       `json:"kind,omitempty"`
	Step           string       `json:"step,omitempty"`
	JobID          string       `json:"jobID,omitempty"`
	IdempotencyKey string       `json:"idempotencyKey,omitempty"`
	NodeID         string       `json:"nodeID,omitempty"`
	Status         string       `json:"status,omitempty"`
	Message        string       `json:"message,omitempty"`
	StartedAt      *metav1.Time `json:"startedAt,omitempty"`
	FinishedAt     *metav1.Time `json:"finishedAt,omitempty"`
}

type PirinDBClusterStatus struct {
	ObservedGeneration  int64                             `json:"observedGeneration,omitempty"`
	Phase               string                            `json:"phase,omitempty"`
	CurrentReplicas     int32                             `json:"currentReplicas,omitempty"`
	ReadyReplicas       int32                             `json:"readyReplicas,omitempty"`
	TopologyHash        string                            `json:"topologyHash,omitempty"`
	RuntimeTopologyHash string                            `json:"runtimeTopologyHash,omitempty"`
	RuntimeEpoch        uint64                            `json:"runtimeEpoch,omitempty"`
	TopologyReplicas    int32                             `json:"topologyReplicas,omitempty"`
	RuntimeConditions   []PirinDBClusterRuntimeCondition  `json:"runtimeConditions,omitempty"`
	RuntimeNodes        []PirinDBClusterRuntimeNodeStatus `json:"runtimeNodes,omitempty"`
	ActiveOperation     *PirinDBClusterOperationStatus    `json:"activeOperation,omitempty"`
	Conditions          []metav1.Condition                `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=pdb
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.status.currentReplicas`
type PirinDBCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PirinDBClusterSpec   `json:"spec,omitempty"`
	Status PirinDBClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type PirinDBClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PirinDBCluster `json:"items"`
}
