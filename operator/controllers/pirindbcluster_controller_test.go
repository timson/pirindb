package controllers

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pirindbv1alpha1 "github.com/timson/pirindb/operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeClusterAdmin struct {
	getStatusResponse          *clusterAPIStatusResponse
	reconcileTopologyResponse  *clusterAPIStatusResponse
	autoJobResponse            *clusterAPIAutoJobResponse
	autoJobPollResponse        *clusterAPIAutoJobResponse
	drainJobResponse           *clusterAPIAutoJobResponse
	drainJobPollResponse       *clusterAPIAutoJobResponse
	removedNodeID              string
	autoDestinationNodeID      string
	drainNodeID                string
	reconcileTopologyCallCount int
	getStatusCallCount         int
	autoStartCallCount         int
	autoPollCallCount          int
	drainStartCallCount        int
	drainPollCallCount         int
	removeCallCount            int
	bearerToken                string
	getStatusByURL             map[string]*clusterAPIStatusResponse
	reconcileTopologyByURL     map[string]*clusterAPIStatusResponse
	getStatusURLs              []string
	reconcileTopologyURLs      []string
}

func (f *fakeClusterAdmin) SetBearerToken(token string) {
	f.bearerToken = token
}

func (f *fakeClusterAdmin) GetStatus(_ context.Context, baseURL string) (*clusterAPIStatusResponse, error) {
	f.getStatusCallCount++
	f.getStatusURLs = append(f.getStatusURLs, baseURL)
	if response, ok := f.getStatusByURL[baseURL]; ok {
		return response, nil
	}
	return f.getStatusResponse, nil
}

func (f *fakeClusterAdmin) ReconcileTopology(_ context.Context, baseURL string) (*clusterAPIStatusResponse, error) {
	f.reconcileTopologyCallCount++
	f.reconcileTopologyURLs = append(f.reconcileTopologyURLs, baseURL)
	if response, ok := f.reconcileTopologyByURL[baseURL]; ok {
		return response, nil
	}
	return f.reconcileTopologyResponse, nil
}

func (f *fakeClusterAdmin) StartAutoRebalance(_ context.Context, _ string, destinationNodeID, _ string) (*clusterAPIAutoJobResponse, error) {
	f.autoStartCallCount++
	f.autoDestinationNodeID = destinationNodeID
	return f.autoJobResponse, nil
}

func (f *fakeClusterAdmin) GetAutoRebalanceJob(_ context.Context, _ string, _ string) (*clusterAPIAutoJobResponse, error) {
	f.autoPollCallCount++
	return f.autoJobPollResponse, nil
}

func (f *fakeClusterAdmin) StartDrain(_ context.Context, _ string, nodeID, _ string) (*clusterAPIAutoJobResponse, error) {
	f.drainStartCallCount++
	f.drainNodeID = nodeID
	return f.drainJobResponse, nil
}

func (f *fakeClusterAdmin) GetDrainJob(_ context.Context, _ string, _ string) (*clusterAPIAutoJobResponse, error) {
	f.drainPollCallCount++
	return f.drainJobPollResponse, nil
}

func (f *fakeClusterAdmin) RemoveNode(_ context.Context, _ string, nodeID string) error {
	f.removeCallCount++
	f.removedNodeID = nodeID
	return nil
}

func TestReconcileScaleUpStartsAutoRebalance(t *testing.T) {
	t.Parallel()

	reconciler, clusterName := newTestReconciler(t,
		newCluster("default", "demo", 3),
		newStatefulSet("default", "demo", 3, 3),
	)
	admin := &fakeClusterAdmin{
		getStatusResponse: &clusterAPIStatusResponse{
			LocalNodeID: "demo-0",
			Conditions: []clusterAPICondition{
				{Type: "TopologyAligned", Status: "True", Reason: "HashMatch"},
				{Type: "SlotBalanced", Status: "False", Reason: "UnevenSlotOwnership"},
			},
			NodeStatuses: []clusterAPINodeStatus{
				{NodeID: "demo-0", OwnedSlots: 8192, TargetSlots: 5461},
				{NodeID: "demo-1", OwnedSlots: 8192, TargetSlots: 5461},
				{NodeID: "demo-2", OwnedSlots: 0, TargetSlots: 5462},
			},
		},
		reconcileTopologyResponse: &clusterAPIStatusResponse{
			LocalNodeID: "demo-0",
			Conditions: []clusterAPICondition{
				{Type: "TopologyAligned", Status: "True", Reason: "HashMatch"},
				{Type: "SlotBalanced", Status: "False", Reason: "UnevenSlotOwnership"},
			},
			NodeStatuses: []clusterAPINodeStatus{
				{NodeID: "demo-0", OwnedSlots: 8192, TargetSlots: 5461},
				{NodeID: "demo-1", OwnedSlots: 8192, TargetSlots: 5461},
				{NodeID: "demo-2", OwnedSlots: 0, TargetSlots: 5462},
			},
		},
		autoJobResponse: &clusterAPIAutoJobResponse{JobID: "auto-1", Status: "queued", Kind: "scale-out"},
	}
	reconciler.ClusterAdmin = admin

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: clusterName})
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, result.RequeueAfter)
	require.Equal(t, 1, admin.autoStartCallCount)
	require.Equal(t, "demo-2", admin.autoDestinationNodeID)

	var cluster pirindbv1alpha1.PirinDBCluster
	require.NoError(t, reconciler.Get(context.Background(), clusterName, &cluster))
	require.NotNil(t, cluster.Status.ActiveOperation)
	require.Equal(t, pirinDBClusterKindScaleOut, cluster.Status.ActiveOperation.Kind)
	require.Equal(t, "auto-1", cluster.Status.ActiveOperation.JobID)
	require.Equal(t, "demo-2", cluster.Status.ActiveOperation.NodeID)
}

func TestReconcileScaleDownStartsDrain(t *testing.T) {
	t.Parallel()

	reconciler, clusterName := newTestReconciler(t,
		newCluster("default", "demo", 2),
		newStatefulSet("default", "demo", 3, 3),
	)
	admin := &fakeClusterAdmin{
		getStatusResponse: &clusterAPIStatusResponse{
			LocalNodeID: "demo-0",
			Conditions: []clusterAPICondition{
				{Type: "TopologyAligned", Status: "True"},
				{Type: "SlotBalanced", Status: "False"},
			},
			NodeStatuses: []clusterAPINodeStatus{
				{NodeID: "demo-0", OwnedSlots: 5000, TargetSlots: 8192},
				{NodeID: "demo-1", OwnedSlots: 5000, TargetSlots: 8192},
				{NodeID: "demo-2", OwnedSlots: 6384, TargetSlots: 0},
			},
		},
		reconcileTopologyResponse: &clusterAPIStatusResponse{
			LocalNodeID: "demo-0",
			Conditions: []clusterAPICondition{
				{Type: "TopologyAligned", Status: "True"},
				{Type: "SlotBalanced", Status: "False"},
			},
			NodeStatuses: []clusterAPINodeStatus{
				{NodeID: "demo-0", OwnedSlots: 5000, TargetSlots: 8192},
				{NodeID: "demo-1", OwnedSlots: 5000, TargetSlots: 8192},
				{NodeID: "demo-2", OwnedSlots: 6384, TargetSlots: 0},
			},
		},
		drainJobResponse: &clusterAPIAutoJobResponse{JobID: "drain-1", Status: "queued", Kind: "drain"},
	}
	reconciler.ClusterAdmin = admin

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: clusterName})
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, result.RequeueAfter)
	require.Equal(t, 1, admin.drainStartCallCount)
	require.Equal(t, "demo-2", admin.drainNodeID)

	var cluster pirindbv1alpha1.PirinDBCluster
	require.NoError(t, reconciler.Get(context.Background(), clusterName, &cluster))
	require.NotNil(t, cluster.Status.ActiveOperation)
	require.Equal(t, pirinDBClusterKindDrain, cluster.Status.ActiveOperation.Kind)
	require.Equal(t, "drain-1", cluster.Status.ActiveOperation.JobID)

	var sts appsv1.StatefulSet
	require.NoError(t, reconciler.Get(context.Background(), clusterName, &sts))
	require.NotNil(t, sts.Spec.Replicas)
	require.Equal(t, int32(3), *sts.Spec.Replicas)
	require.Equal(t, 0, admin.reconcileTopologyCallCount)
	var topology corev1.ConfigMap
	require.NoError(t, reconciler.Get(context.Background(), types.NamespacedName{Name: "demo-topology", Namespace: "default"}, &topology))
	require.Equal(t, 3, strings.Count(topology.Data[pirinDBTopologyConfigMapKey], "[[cluster.nodes]]"))
}

func TestReconcileScaleDownWaitsForSmallerTopologyBeforeShrinking(t *testing.T) {
	t.Parallel()

	reconciler, clusterName := newTestReconciler(t,
		newCluster("default", "demo", 2),
		newStatefulSet("default", "demo", 3, 3),
	)
	admin := &fakeClusterAdmin{
		getStatusResponse: &clusterAPIStatusResponse{
			LocalNodeID: "demo-0",
			Conditions:  []clusterAPICondition{{Type: "TopologyAligned", Status: "True"}},
			NodeStatuses: []clusterAPINodeStatus{
				{NodeID: "demo-0", OwnedSlots: 8192, TargetSlots: 8192},
				{NodeID: "demo-1", OwnedSlots: 8192, TargetSlots: 8192},
				{NodeID: "demo-2", OwnedSlots: 0, TargetSlots: 0},
			},
		},
		reconcileTopologyResponse: &clusterAPIStatusResponse{
			LocalNodeID:       "demo-0",
			ConfiguredNodeIDs: []string{"demo-0", "demo-1", "demo-2"},
			Conditions:        []clusterAPICondition{{Type: "TopologyAligned", Status: "True"}},
			NodeStatuses: []clusterAPINodeStatus{
				{NodeID: "demo-0", OwnedSlots: 8192, TargetSlots: 8192},
				{NodeID: "demo-1", OwnedSlots: 8192, TargetSlots: 8192},
				{NodeID: "demo-2", OwnedSlots: 0, TargetSlots: 0},
			},
		},
	}
	reconciler.ClusterAdmin = admin

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: clusterName})
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, result.RequeueAfter)
	var sts appsv1.StatefulSet
	require.NoError(t, reconciler.Get(context.Background(), clusterName, &sts))
	require.Equal(t, int32(3), *sts.Spec.Replicas)
	var cluster pirindbv1alpha1.PirinDBCluster
	require.NoError(t, reconciler.Get(context.Background(), clusterName, &cluster))
	require.Equal(t, int32(2), cluster.Status.TopologyReplicas)
	require.Equal(t, "WaitingForTopologyRevision", cluster.Status.Phase)

	admin.reconcileTopologyResponse = &clusterAPIStatusResponse{
		LocalNodeID:       "demo-0",
		ConfiguredNodeIDs: []string{"demo-0", "demo-1"},
		Conditions:        []clusterAPICondition{{Type: "TopologyAligned", Status: "True"}},
		NodeStatuses: []clusterAPINodeStatus{
			{NodeID: "demo-0", OwnedSlots: 8192, TargetSlots: 8192},
			{NodeID: "demo-1", OwnedSlots: 8192, TargetSlots: 8192},
		},
	}
	admin.getStatusResponse = admin.reconcileTopologyResponse
	result, err = reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: clusterName})
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, result.RequeueAfter)
	require.NoError(t, reconciler.Get(context.Background(), clusterName, &sts))
	require.Equal(t, int32(2), *sts.Spec.Replicas)
}

func TestReconcileTopologyAcrossNodesWaitsForEveryProjectedConfig(t *testing.T) {
	t.Parallel()
	cluster := newCluster("default", "demo", 2)
	urls := map[int32]string{0: "http://demo-0", 1: "http://demo-1"}
	aligned := func(localNodeID string) *clusterAPIStatusResponse {
		return &clusterAPIStatusResponse{
			ClusterID:              "cluster-a",
			Epoch:                  9,
			LocalNodeID:            localNodeID,
			RuntimeTopologyHash:    "hash-2",
			ConfiguredTopologyName: "default/demo",
			ConfiguredTopologyHash: "hash-2",
			ConfiguredNodeIDs:      []string{"demo-0", "demo-1"},
			Conditions:             []clusterAPICondition{{Type: "TopologyAligned", Status: "True"}},
			NodeStatuses: []clusterAPINodeStatus{
				{NodeID: "demo-0"},
				{NodeID: "demo-1"},
			},
		}
	}
	stale := aligned("demo-1")
	stale.ConfiguredTopologyHash = "hash-3"
	stale.ConfiguredNodeIDs = []string{"demo-0", "demo-1", "demo-2"}
	stale.Conditions = []clusterAPICondition{{Type: "TopologyAligned", Status: "False"}}
	admin := &fakeClusterAdmin{
		getStatusByURL: map[string]*clusterAPIStatusResponse{
			urls[0]: aligned("demo-0"),
			urls[1]: stale,
		},
		reconcileTopologyByURL: map[string]*clusterAPIStatusResponse{
			urls[0]: aligned("demo-0"),
			urls[1]: aligned("demo-1"),
		},
	}
	reconciler := &PirinDBClusterReconciler{
		ClusterAdmin: admin,
		ResolveNodeBaseURL: func(_ *pirindbv1alpha1.PirinDBCluster, ordinal int32) string {
			return urls[ordinal]
		},
	}

	_, converged, err := reconciler.reconcileTopologyAcrossNodes(context.Background(), cluster, 2)
	require.NoError(t, err)
	require.False(t, converged)
	require.Zero(t, admin.reconcileTopologyCallCount, "a stale projected file must never be reconciled")

	admin.getStatusByURL[urls[1]] = aligned("demo-1")
	status, converged, err := reconciler.reconcileTopologyAcrossNodes(context.Background(), cluster, 2)
	require.NoError(t, err)
	require.True(t, converged)
	require.Equal(t, "demo-0", status.LocalNodeID)
	require.Equal(t, []string{urls[0], urls[1]}, admin.reconcileTopologyURLs)
}

func TestReconcileScaleDownShrinksStatefulSetAfterRuntimeNodeIsGone(t *testing.T) {
	t.Parallel()

	reconciler, clusterName := newTestReconciler(t,
		newCluster("default", "demo", 2),
		newStatefulSet("default", "demo", 3, 3),
	)
	admin := &fakeClusterAdmin{
		getStatusResponse: &clusterAPIStatusResponse{
			LocalNodeID: "demo-0",
			Conditions: []clusterAPICondition{
				{Type: "TopologyAligned", Status: "True"},
				{Type: "SlotBalanced", Status: "True"},
			},
			NodeStatuses: []clusterAPINodeStatus{
				{NodeID: "demo-0", OwnedSlots: 8192, TargetSlots: 8192},
				{NodeID: "demo-1", OwnedSlots: 8192, TargetSlots: 8192},
			},
		},
		reconcileTopologyResponse: &clusterAPIStatusResponse{
			LocalNodeID: "demo-0",
			Conditions: []clusterAPICondition{
				{Type: "TopologyAligned", Status: "True"},
				{Type: "SlotBalanced", Status: "True"},
			},
			NodeStatuses: []clusterAPINodeStatus{
				{NodeID: "demo-0", OwnedSlots: 8192, TargetSlots: 8192},
				{NodeID: "demo-1", OwnedSlots: 8192, TargetSlots: 8192},
			},
		},
	}
	reconciler.ClusterAdmin = admin

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: clusterName})
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, result.RequeueAfter)

	var sts appsv1.StatefulSet
	require.NoError(t, reconciler.Get(context.Background(), clusterName, &sts))
	require.NotNil(t, sts.Spec.Replicas)
	require.Equal(t, int32(2), *sts.Spec.Replicas)
}

func TestReconcilePollsExistingActiveOperation(t *testing.T) {
	t.Parallel()

	cluster := newCluster("default", "demo", 3)
	now := metav1.Now()
	cluster.Status.ActiveOperation = &pirindbv1alpha1.PirinDBClusterOperationStatus{
		Kind:      pirinDBClusterKindScaleOut,
		JobID:     "auto-1",
		NodeID:    "demo-2",
		Status:    "running",
		StartedAt: &now,
	}
	reconciler, clusterName := newTestReconciler(t,
		cluster,
		newStatefulSet("default", "demo", 3, 3),
	)
	admin := &fakeClusterAdmin{
		autoJobPollResponse: &clusterAPIAutoJobResponse{JobID: "auto-1", Status: "running", Kind: "scale-out"},
	}
	reconciler.ClusterAdmin = admin

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: clusterName})
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, result.RequeueAfter)
	require.Equal(t, 1, admin.autoPollCallCount)

	var updated pirindbv1alpha1.PirinDBCluster
	require.NoError(t, reconciler.Get(context.Background(), clusterName, &updated))
	require.NotNil(t, updated.Status.ActiveOperation)
	require.Equal(t, "running", updated.Status.ActiveOperation.Status)
}

func TestConfigureClusterAdminAuthUsesUncachedSecretReader(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, pirindbv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	secretReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-admin", Namespace: "default"},
		Data:       map[string][]byte{"token": []byte("uncached-control-plane-token")},
	}).Build()
	admin := &fakeClusterAdmin{}
	reconciler := &PirinDBClusterReconciler{
		Client:       cachedClient,
		APIReader:    secretReader,
		Scheme:       scheme,
		ClusterAdmin: admin,
	}
	cluster := newCluster("default", "demo", 3)
	cluster.Spec.AdminSecretRef = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "cluster-admin"},
		Key:                  "token",
	}

	require.NoError(t, reconciler.configureClusterAdminAuth(context.Background(), cluster))
	require.Equal(t, "uncached-control-plane-token", admin.bearerToken)
	var absent corev1.Secret
	require.Error(t, cachedClient.Get(context.Background(), types.NamespacedName{Name: "cluster-admin", Namespace: "default"}, &absent))
}

func TestOperatorRequiresAdminSecretAndValidStorageQuantity(t *testing.T) {
	t.Parallel()
	cluster := newCluster("default", "demo", 3)
	cluster.Spec.AdminSecretRef = nil
	reconciler := &PirinDBClusterReconciler{ClusterAdmin: &fakeClusterAdmin{}}
	require.ErrorContains(t, reconciler.configureClusterAdminAuth(context.Background(), cluster), "adminSecretRef is required")

	cluster.Spec.Storage.Size = "not-a-quantity"
	_, err := buildDataPVC(cluster)
	require.ErrorContains(t, err, "not a positive Kubernetes quantity")
	cluster.Spec.Storage.Size = "20Gi"
	pvc, err := buildDataPVC(cluster)
	require.NoError(t, err)
	require.Equal(t, "20Gi", pvc.Spec.Resources.Requests.Storage().String())
}

func newTestReconciler(t *testing.T, objects ...runtime.Object) (*PirinDBClusterReconciler, types.NamespacedName) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, pirindbv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	runtimeObjects := append([]runtime.Object{&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-cluster-admin", Namespace: "default"},
		Data:       map[string][]byte{"token": []byte("test-control-plane-token")},
	}}, objects...)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&pirindbv1alpha1.PirinDBCluster{}).
		WithRuntimeObjects(runtimeObjects...).
		Build()
	reconciler := &PirinDBClusterReconciler{
		Client:         cl,
		Scheme:         scheme,
		ResolveBaseURL: func(_ *pirindbv1alpha1.PirinDBCluster) string { return "http://cluster" },
		Now:            func() time.Time { return time.Unix(1700000000, 0).UTC() },
	}
	return reconciler, types.NamespacedName{Name: "demo", Namespace: "default"}
}

func newCluster(namespace, name string, replicas int32) *pirindbv1alpha1.PirinDBCluster {
	return &pirindbv1alpha1.PirinDBCluster{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  namespace,
			Name:       name,
			Finalizers: []string{pirinDBClusterFinalizerName},
		},
		Spec: pirindbv1alpha1.PirinDBClusterSpec{
			Replicas: &replicas,
			Image:    "ghcr.io/timson/pirindb:latest",
			AdminSecretRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "demo-cluster-admin"},
				Key:                  "token",
			},
		},
	}
}

func newStatefulSet(namespace, name string, replicas, readyReplicas int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
		},
		Status: appsv1.StatefulSetStatus{
			ReadyReplicas: readyReplicas,
		},
	}
}

func TestStatefulSetAllowsRecoveryBeforeHealthProbes(t *testing.T) {
	cluster := newCluster("default", "demo", 3)
	reconciler, _ := newTestReconciler(t, cluster)
	sts, err := reconciler.ensureStatefulSet(context.Background(), cluster, "demo-headless", 3)
	require.NoError(t, err)
	container := sts.Spec.Template.Spec.Containers[0]
	require.NotNil(t, container.StartupProbe)
	require.Equal(t, "/health", container.StartupProbe.HTTPGet.Path)
	require.GreaterOrEqual(t, container.StartupProbe.PeriodSeconds*container.StartupProbe.FailureThreshold, int32(600))
	require.Equal(t, "/health", container.ReadinessProbe.HTTPGet.Path)
	require.Equal(t, "/health", container.LivenessProbe.HTTPGet.Path)
}
