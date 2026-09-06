package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	pirindbv1alpha1 "github.com/timson/pirindb/operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	pirinDBClusterKindScaleOut = "scale-out"
	pirinDBClusterKindDrain    = "drain"

	pirinDBClusterConditionResourcesReady        = "ResourcesReady"
	pirinDBClusterConditionClusterAPIReady       = "ClusterAPIReady"
	pirinDBClusterConditionTopologyAligned       = "TopologyAligned"
	pirinDBClusterConditionSlotBalanced          = "SlotBalanced"
	pirinDBClusterConditionProgressing           = "Progressing"
	pirinDBClusterConditionDegraded              = "Degraded"
	pirinDBClusterConditionActiveOperation       = "ActiveOperation"
	pirinDBClusterFinalizerName                  = "pirindb.timson.dev/finalizer"
	pirinDBTopologyConfigMapKey                  = "topology.toml"
	pirinDBDefaultStorageSize                    = "10Gi"
	pirinDBDefaultHTTPPort                 int32 = 4321
	pirinDBDefaultRedisPort                int32 = 6379
	pirinDBDefaultSlotCount                int32 = 16384
	pirinDBDefaultReplicas                 int32 = 1
)

type PirinDBClusterReconciler struct {
	client.Client
	Scheme             *runtime.Scheme
	APIReader          client.Reader
	ClusterAdmin       ClusterAdminClient
	ResolveBaseURL     func(cluster *pirindbv1alpha1.PirinDBCluster) string
	ResolveNodeBaseURL func(cluster *pirindbv1alpha1.PirinDBCluster, ordinal int32) string
	Now                func() time.Time
}

func (r *PirinDBClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, retErr error) {
	_ = log.FromContext(ctx)

	var cluster pirindbv1alpha1.PirinDBCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	statusBase := cluster.DeepCopy()
	defer func() {
		if patchErr := r.patchStatus(ctx, &cluster, statusBase); patchErr != nil && retErr == nil {
			retErr = patchErr
		}
	}()

	if !cluster.DeletionTimestamp.IsZero() {
		controllerutil.RemoveFinalizer(&cluster, pirinDBClusterFinalizerName)
		if err := r.Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&cluster, pirinDBClusterFinalizerName) {
		controllerutil.AddFinalizer(&cluster, pirinDBClusterFinalizerName)
		if err := r.Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if r.ClusterAdmin == nil {
		r.ClusterAdmin = NewHTTPClusterAdminClient()
	}
	if r.ResolveBaseURL == nil {
		r.ResolveBaseURL = defaultClusterBaseURL
	}
	if r.ResolveNodeBaseURL == nil {
		r.ResolveNodeBaseURL = defaultClusterNodeBaseURL
	}
	if r.Now == nil {
		r.Now = time.Now
	}
	if err := r.configureClusterAdminAuth(ctx, &cluster); err != nil {
		r.setDegradedCondition(&cluster, "ClusterAdminSecretFailed", err.Error())
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	desiredReplicas := clusterDesiredReplicas(&cluster)
	cluster.Status.ObservedGeneration = cluster.Generation

	headlessSvc, err := r.ensureHeadlessService(ctx, &cluster)
	if err != nil {
		r.setDegradedCondition(&cluster, "EnsureHeadlessServiceFailed", err.Error())
		return ctrl.Result{}, err
	}
	currentReplicas := desiredReplicas
	existingReplicas := desiredReplicas
	statefulSetExists := false
	var existingSTS appsv1.StatefulSet
	stsKey := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	if err = r.Get(ctx, stsKey, &existingSTS); err == nil {
		statefulSetExists = true
		if existingSTS.Spec.Replicas != nil {
			existingReplicas = *existingSTS.Spec.Replicas
		}
		currentReplicas = existingReplicas
	} else if !apierrors.IsNotFound(err) {
		r.setDegradedCondition(&cluster, "GetStatefulSetFailed", err.Error())
		return ctrl.Result{}, err
	}

	topologyReplicas := desiredReplicas
	if statefulSetExists && existingReplicas > desiredReplicas {
		topologyReplicas = existingReplicas
		if cluster.Status.TopologyReplicas >= desiredReplicas && cluster.Status.TopologyReplicas < existingReplicas {
			topologyReplicas = cluster.Status.TopologyReplicas
		}
	}
	topologyContent := renderClusterTopology(&cluster, topologyReplicas)
	cluster.Status.TopologyReplicas = topologyReplicas
	if err = r.ensureTopologyConfigMap(ctx, &cluster, topologyContent); err != nil {
		r.setDegradedCondition(&cluster, "EnsureTopologyConfigMapFailed", err.Error())
		return ctrl.Result{}, err
	}

	if existingReplicas < desiredReplicas {
		currentReplicas = desiredReplicas
	}

	sts, err := r.ensureStatefulSet(ctx, &cluster, headlessSvc.Name, currentReplicas)
	if err != nil {
		r.setDegradedCondition(&cluster, "EnsureStatefulSetFailed", err.Error())
		return ctrl.Result{}, err
	}
	cluster.Status.CurrentReplicas = 0
	cluster.Status.ReadyReplicas = 0
	if sts.Spec.Replicas != nil {
		cluster.Status.CurrentReplicas = *sts.Spec.Replicas
	}
	cluster.Status.ReadyReplicas = sts.Status.ReadyReplicas

	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               pirinDBClusterConditionResourcesReady,
		Status:             conditionStatus(cluster.Status.ReadyReplicas == cluster.Status.CurrentReplicas),
		ObservedGeneration: cluster.Generation,
		Reason:             resourcesReadyReason(cluster.Status.ReadyReplicas == cluster.Status.CurrentReplicas),
		Message:            fmt.Sprintf("readyReplicas=%d currentReplicas=%d", cluster.Status.ReadyReplicas, cluster.Status.CurrentReplicas),
	})

	if cluster.Status.ActiveOperation != nil {
		done, opErr := r.pollActiveOperation(ctx, &cluster)
		if opErr != nil {
			r.setDegradedCondition(&cluster, "PollActiveOperationFailed", opErr.Error())
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		if !done {
			cluster.Status.Phase = "Rebalancing"
			r.setProgressingCondition(&cluster, "ActiveOperationRunning", "operator is waiting for the active rebalance job to finish")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	if cluster.Status.CurrentReplicas == 0 || cluster.Status.ReadyReplicas == 0 {
		cluster.Status.Phase = "Pending"
		r.setClusterAPICondition(&cluster, metav1.ConditionFalse, "NoReadyPods", "waiting for at least one ready pod")
		r.setProgressingCondition(&cluster, "WaitingForPods", "waiting for cluster pods to become ready")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	baseURL := r.ResolveBaseURL(&cluster)
	runtimeStatus, err := r.ClusterAdmin.GetStatus(ctx, baseURL)
	if err != nil {
		cluster.Status.Phase = "Pending"
		r.setClusterAPICondition(&cluster, metav1.ConditionFalse, "ClusterStatusFetchFailed", err.Error())
		r.setProgressingCondition(&cluster, "WaitingForClusterAPI", "cluster API is not reachable yet")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	r.setClusterAPICondition(&cluster, metav1.ConditionTrue, "ClusterStatusFetched", "cluster API is reachable")
	r.syncRuntimeStatus(&cluster, runtimeStatus)

	if cluster.Status.CurrentReplicas > desiredReplicas {
		return r.reconcileScaleDown(ctx, &cluster, sts, desiredReplicas, baseURL)
	}

	if cluster.Status.ReadyReplicas == cluster.Status.CurrentReplicas {
		var converged bool
		runtimeStatus, converged, err = r.reconcileTopologyAcrossNodes(ctx, &cluster, cluster.Status.CurrentReplicas)
		if err != nil {
			r.setDegradedCondition(&cluster, "ReconcileTopologyFailed", err.Error())
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		if runtimeStatus != nil {
			r.syncRuntimeStatus(&cluster, runtimeStatus)
		}
		if !converged {
			cluster.Status.Phase = "WaitingForTopologyRevision"
			r.setProgressingCondition(&cluster, "WaitingForTopologyRevision", "waiting for every PirinDB pod to load and reconcile the expected topology membership")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	if cluster.Status.CurrentReplicas < desiredReplicas {
		cluster.Status.Phase = "ScalingUp"
		r.setProgressingCondition(&cluster, "StatefulSetScalingUp", "waiting for StatefulSet to reach desired replicas")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	if cluster.Status.ReadyReplicas < desiredReplicas {
		cluster.Status.Phase = "ScalingUp"
		r.setProgressingCondition(&cluster, "WaitingForPods", "waiting for all desired pods to become ready")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	if runtimeStatus.Rebalancing {
		cluster.Status.Phase = "Rebalancing"
		r.setProgressingCondition(&cluster, "ClusterRebalancing", "cluster runtime is executing a rebalance")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	topologyAligned := runtimeConditionTrue(runtimeStatus.Conditions, "TopologyAligned")
	slotBalanced := runtimeConditionTrue(runtimeStatus.Conditions, "SlotBalanced")
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               pirinDBClusterConditionTopologyAligned,
		Status:             conditionStatus(topologyAligned),
		ObservedGeneration: cluster.Generation,
		Reason:             runtimeConditionReason(runtimeStatus.Conditions, "TopologyAligned", "Unknown"),
		Message:            runtimeConditionMessage(runtimeStatus.Conditions, "TopologyAligned", ""),
	})
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               pirinDBClusterConditionSlotBalanced,
		Status:             conditionStatus(slotBalanced),
		ObservedGeneration: cluster.Generation,
		Reason:             runtimeConditionReason(runtimeStatus.Conditions, "SlotBalanced", "Unknown"),
		Message:            runtimeConditionMessage(runtimeStatus.Conditions, "SlotBalanced", ""),
	})

	if !topologyAligned {
		cluster.Status.Phase = "ReconcilingTopology"
		r.setProgressingCondition(&cluster, "TopologyNotAligned", "waiting for cluster runtime topology to align with the mounted topology file")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	if !slotBalanced && clusterAutoRebalanceOnScaleUp(&cluster) {
		destinationNodeID := pickScaleOutDestinationNode(runtimeStatus.NodeStatuses)
		if destinationNodeID != "" {
			idempotencyKey := clusterOperationIdempotencyKey(&cluster, pirinDBClusterKindScaleOut, destinationNodeID)
			job, startErr := r.ClusterAdmin.StartAutoRebalance(ctx, baseURL, destinationNodeID, idempotencyKey)
			if startErr != nil {
				r.setDegradedCondition(&cluster, "StartAutoRebalanceFailed", startErr.Error())
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
			now := metav1.NewTime(r.Now().UTC())
			cluster.Status.ActiveOperation = &pirindbv1alpha1.PirinDBClusterOperationStatus{
				Kind:           pirinDBClusterKindScaleOut,
				Step:           "Rebalancing",
				JobID:          job.JobID,
				IdempotencyKey: idempotencyKey,
				NodeID:         destinationNodeID,
				Status:         job.Status,
				Message:        "waiting for automatic rebalance",
				StartedAt:      &now,
			}
			cluster.Status.Phase = "Rebalancing"
			r.setProgressingCondition(&cluster, "ScaleOutRebalanceStarted", fmt.Sprintf("started auto rebalance for %s", destinationNodeID))
			meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
				Type:               pirinDBClusterConditionActiveOperation,
				Status:             metav1.ConditionTrue,
				ObservedGeneration: cluster.Generation,
				Reason:             "AutoRebalanceStarted",
				Message:            fmt.Sprintf("auto rebalance job %s is running for node %s", job.JobID, destinationNodeID),
			})
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	if !slotBalanced {
		cluster.Status.Phase = "WaitingForBalance"
		r.setProgressingCondition(&cluster, "SlotBalancePending", "cluster runtime is not yet slot-balanced")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	cluster.Status.Phase = "Ready"
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               pirinDBClusterConditionProgressing,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: cluster.Generation,
		Reason:             "SteadyState",
		Message:            "cluster is reconciled and balanced",
	})
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               pirinDBClusterConditionDegraded,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: cluster.Generation,
		Reason:             "AsExpected",
		Message:            "cluster is healthy",
	})
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               pirinDBClusterConditionActiveOperation,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: cluster.Generation,
		Reason:             "NoActiveOperation",
		Message:            "no active rebalance job",
	})
	return ctrl.Result{}, nil
}

func (r *PirinDBClusterReconciler) reconcileScaleDown(ctx context.Context, cluster *pirindbv1alpha1.PirinDBCluster, sts *appsv1.StatefulSet, desiredReplicas int32, baseURL string) (ctrl.Result, error) {
	cluster.Status.Phase = "ScalingDown"
	if cluster.Status.ReadyReplicas < cluster.Status.CurrentReplicas {
		r.setProgressingCondition(cluster, "WaitingForPods", "waiting for all current pods to become ready before draining a node")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	candidateOrdinal := cluster.Status.CurrentReplicas - 1
	candidateID := statefulSetNodeID(cluster.Name, candidateOrdinal)
	nodeStatus, found := findRuntimeNodeStatus(cluster.Status.RuntimeNodes, candidateID)
	if !found {
		targetTopologyReplicas := cluster.Status.CurrentReplicas - 1
		if targetTopologyReplicas < desiredReplicas {
			targetTopologyReplicas = desiredReplicas
		}
		cluster.Status.TopologyReplicas = targetTopologyReplicas
		if err := r.ensureTopologyConfigMap(ctx, cluster, renderClusterTopology(cluster, targetTopologyReplicas)); err != nil {
			r.setDegradedCondition(cluster, "PublishScaleDownTopologyFailed", err.Error())
			return ctrl.Result{}, err
		}
		runtimeStatus, converged, err := r.reconcileTopologyAcrossNodes(ctx, cluster, targetTopologyReplicas)
		if err != nil {
			r.setDegradedCondition(cluster, "ReconcileTopologyFailed", err.Error())
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		if runtimeStatus != nil {
			r.syncRuntimeStatus(cluster, runtimeStatus)
		}
		if !converged {
			cluster.Status.Phase = "WaitingForTopologyRevision"
			r.setProgressingCondition(cluster, "WaitingForTopologyRevision", fmt.Sprintf("waiting for every remaining pod to load topology without %s", candidateID))
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		if err := r.scaleStatefulSet(ctx, cluster, sts, targetTopologyReplicas); err != nil {
			r.setDegradedCondition(cluster, "ScaleStatefulSetDownFailed", err.Error())
			return ctrl.Result{}, err
		}
		r.setProgressingCondition(cluster, "StatefulSetScalingDown", fmt.Sprintf("shrinking StatefulSet after removing %s from cluster membership", candidateID))
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	if nodeStatus.OwnedSlots > 0 {
		if cluster.Status.TopologyReplicas != cluster.Status.CurrentReplicas {
			cluster.Status.TopologyReplicas = cluster.Status.CurrentReplicas
			if err := r.ensureTopologyConfigMap(ctx, cluster, renderClusterTopology(cluster, cluster.Status.CurrentReplicas)); err != nil {
				r.setDegradedCondition(cluster, "RestoreDrainTopologyFailed", err.Error())
				return ctrl.Result{}, err
			}
			r.setProgressingCondition(cluster, "RestoringDrainTopology", "restoring current membership before draining a slot-owning node")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		if !clusterAutoRebalanceOnScaleDown(cluster) {
			r.setDegradedCondition(cluster, "ScaleDownBlocked", fmt.Sprintf("node %s still owns %d slots and auto drain is disabled", candidateID, nodeStatus.OwnedSlots))
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}
		idempotencyKey := clusterOperationIdempotencyKey(cluster, pirinDBClusterKindDrain, candidateID)
		job, err := r.ClusterAdmin.StartDrain(ctx, baseURL, candidateID, idempotencyKey)
		if err != nil {
			r.setDegradedCondition(cluster, "StartDrainFailed", err.Error())
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		now := metav1.NewTime(r.Now().UTC())
		cluster.Status.ActiveOperation = &pirindbv1alpha1.PirinDBClusterOperationStatus{
			Kind:           pirinDBClusterKindDrain,
			Step:           "Draining",
			JobID:          job.JobID,
			IdempotencyKey: idempotencyKey,
			NodeID:         candidateID,
			Status:         job.Status,
			Message:        "waiting for node drain",
			StartedAt:      &now,
		}
		r.setProgressingCondition(cluster, "DrainStarted", fmt.Sprintf("started drain job %s for node %s", job.JobID, candidateID))
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               pirinDBClusterConditionActiveOperation,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: cluster.Generation,
			Reason:             "DrainStarted",
			Message:            fmt.Sprintf("drain job %s is running for node %s", job.JobID, candidateID),
		})
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	targetTopologyReplicas := cluster.Status.CurrentReplicas - 1
	if targetTopologyReplicas < desiredReplicas {
		targetTopologyReplicas = desiredReplicas
	}
	cluster.Status.TopologyReplicas = targetTopologyReplicas
	cluster.Status.Phase = "PublishingTopology"
	if err := r.ensureTopologyConfigMap(ctx, cluster, renderClusterTopology(cluster, targetTopologyReplicas)); err != nil {
		r.setDegradedCondition(cluster, "PublishScaleDownTopologyFailed", err.Error())
		return ctrl.Result{}, err
	}
	runtimeStatus, converged, err := r.reconcileTopologyAcrossNodes(ctx, cluster, targetTopologyReplicas)
	if err != nil {
		r.setDegradedCondition(cluster, "ReconcileTopologyFailed", err.Error())
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if runtimeStatus != nil {
		r.syncRuntimeStatus(cluster, runtimeStatus)
	}
	if !converged {
		cluster.Status.Phase = "WaitingForTopologyRevision"
		r.setProgressingCondition(cluster, "WaitingForTopologyRevision", fmt.Sprintf("waiting for every remaining pod to load topology without %s", candidateID))
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if _, stillPresent := findRuntimeNodeStatus(cluster.Status.RuntimeNodes, candidateID); stillPresent {
		if err = r.ClusterAdmin.RemoveNode(ctx, baseURL, candidateID); err != nil && !strings.Contains(strings.ToLower(err.Error()), "does not exist") {
			r.setDegradedCondition(cluster, "RemoveNodeFailed", err.Error())
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		cluster.Status.Phase = "RemovingMembership"
		r.setProgressingCondition(cluster, "RemovingMembership", fmt.Sprintf("waiting for %s to disappear from runtime membership", candidateID))
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	if err = r.scaleStatefulSet(ctx, cluster, sts, targetTopologyReplicas); err != nil {
		r.setDegradedCondition(cluster, "ScaleStatefulSetDownFailed", err.Error())
		return ctrl.Result{}, err
	}
	r.setProgressingCondition(cluster, "StatefulSetScalingDown", fmt.Sprintf("drained and removed node %s; shrinking StatefulSet", candidateID))
	if targetTopologyReplicas > desiredReplicas {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func (r *PirinDBClusterReconciler) pollActiveOperation(ctx context.Context, cluster *pirindbv1alpha1.PirinDBCluster) (bool, error) {
	if cluster.Status.ActiveOperation == nil {
		return true, nil
	}
	baseURL := r.ResolveBaseURL(cluster)
	op := cluster.Status.ActiveOperation
	var (
		job *clusterAPIAutoJobResponse
		err error
	)
	switch op.Kind {
	case pirinDBClusterKindScaleOut:
		job, err = r.ClusterAdmin.GetAutoRebalanceJob(ctx, baseURL, op.JobID)
	case pirinDBClusterKindDrain:
		job, err = r.ClusterAdmin.GetDrainJob(ctx, baseURL, op.JobID)
	default:
		cluster.Status.ActiveOperation = nil
		return true, nil
	}
	if err != nil {
		return false, err
	}
	op.Status = job.Status
	op.Message = job.Error
	if job.Status == "queued" || job.Status == "running" {
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               pirinDBClusterConditionActiveOperation,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: cluster.Generation,
			Reason:             "OperationRunning",
			Message:            fmt.Sprintf("%s job %s is %s", op.Kind, op.JobID, job.Status),
		})
		return false, nil
	}
	now := metav1.NewTime(r.Now().UTC())
	op.FinishedAt = &now
	if job.Status == "failed" {
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               pirinDBClusterConditionActiveOperation,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: cluster.Generation,
			Reason:             "OperationFailed",
			Message:            fmt.Sprintf("%s job %s failed: %s", op.Kind, op.JobID, job.Error),
		})
		cluster.Status.ActiveOperation = nil
		return false, fmt.Errorf("%s job %s failed: %s", op.Kind, op.JobID, job.Error)
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               pirinDBClusterConditionActiveOperation,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: cluster.Generation,
		Reason:             "OperationDone",
		Message:            fmt.Sprintf("%s job %s completed", op.Kind, op.JobID),
	})
	cluster.Status.ActiveOperation = nil
	return true, nil
}

func (r *PirinDBClusterReconciler) ensureHeadlessService(ctx context.Context, cluster *pirindbv1alpha1.PirinDBCluster) (*corev1.Service, error) {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: headlessServiceName(cluster), Namespace: cluster.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		service.Labels = clusterLabels(cluster)
		service.Spec.ClusterIP = corev1.ClusterIPNone
		service.Spec.PublishNotReadyAddresses = true
		service.Spec.Selector = clusterLabels(cluster)
		service.Spec.Ports = []corev1.ServicePort{
			{
				Name: "http",
				Port: clusterHTTPPort(cluster),
			},
			{
				Name: "redis",
				Port: clusterRedisPort(cluster),
			},
		}
		return controllerutil.SetControllerReference(cluster, service, r.Scheme)
	})
	return service, err
}

func (r *PirinDBClusterReconciler) ensureTopologyConfigMap(ctx context.Context, cluster *pirindbv1alpha1.PirinDBCluster, topology string) error {
	configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: topologyConfigMapName(cluster), Namespace: cluster.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, configMap, func() error {
		configMap.Labels = clusterLabels(cluster)
		configMap.Data = map[string]string{
			pirinDBTopologyConfigMapKey: topology,
		}
		return controllerutil.SetControllerReference(cluster, configMap, r.Scheme)
	})
	return err
}

func (r *PirinDBClusterReconciler) ensureStatefulSet(ctx context.Context, cluster *pirindbv1alpha1.PirinDBCluster, serviceName string, replicas int32) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: cluster.Name, Namespace: cluster.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		labels := clusterLabels(cluster)
		sts.Labels = labels
		sts.Spec.ServiceName = serviceName
		sts.Spec.Replicas = ptrInt32(replicas)
		sts.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		sts.Spec.Template.ObjectMeta.Labels = labels
		sts.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{
			RunAsNonRoot:   ptrBool(true),
			RunAsUser:      ptrInt64(65532),
			RunAsGroup:     ptrInt64(65532),
			FSGroup:        ptrInt64(65532),
			SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		}
		sts.Spec.Template.Spec.AutomountServiceAccountToken = ptrBool(false)
		sts.Spec.Template.Spec.TerminationGracePeriodSeconds = ptrInt64(45)
		sts.Spec.Template.Spec.Containers = []corev1.Container{
			{
				Name:            "pirindb",
				Image:           cluster.Spec.Image,
				ImagePullPolicy: clusterImagePullPolicy(cluster),
				Args:            buildPirinDBArgs(cluster),
				Ports: []corev1.ContainerPort{
					{Name: "http", ContainerPort: clusterHTTPPort(cluster)},
					{Name: "redis", ContainerPort: clusterRedisPort(cluster)},
				},
				Resources: cluster.Spec.Resources,
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: ptrBool(false),
					ReadOnlyRootFilesystem:   ptrBool(true),
					Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
				},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "topology", MountPath: "/etc/pirindb"},
					{Name: "data", MountPath: "/var/lib/pirindb"},
				},
				StartupProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
						Path: "/health", Port: intstr.FromInt32(clusterHTTPPort(cluster)),
					}},
					PeriodSeconds:    10,
					FailureThreshold: 60,
				},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/health",
							Port: intstr.FromInt32(clusterHTTPPort(cluster)),
						},
					},
					InitialDelaySeconds: 3,
					PeriodSeconds:       5,
				},
				LivenessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/health",
							Port: intstr.FromInt32(clusterHTTPPort(cluster)),
						},
					},
					InitialDelaySeconds: 10,
					PeriodSeconds:       10,
				},
			},
		}
		if cluster.Spec.AdminSecretRef != nil {
			sts.Spec.Template.Spec.Containers[0].Env = []corev1.EnvVar{{
				Name:      "PIRINDB_CLUSTER_ADMIN_TOKEN",
				ValueFrom: &corev1.EnvVarSource{SecretKeyRef: cluster.Spec.AdminSecretRef.DeepCopy()},
			}}
		}
		sts.Spec.Template.Spec.Volumes = []corev1.Volume{
			{
				Name: "topology",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: topologyConfigMapName(cluster)},
					},
				},
			},
		}
		dataPVC, pvcErr := buildDataPVC(cluster)
		if pvcErr != nil {
			return pvcErr
		}
		sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{dataPVC}
		return controllerutil.SetControllerReference(cluster, sts, r.Scheme)
	})
	return sts, err
}

func (r *PirinDBClusterReconciler) scaleStatefulSet(ctx context.Context, cluster *pirindbv1alpha1.PirinDBCluster, sts *appsv1.StatefulSet, replicas int32) error {
	if sts.Spec.Replicas != nil && *sts.Spec.Replicas == replicas {
		return nil
	}
	updated := sts.DeepCopy()
	updated.Spec.Replicas = ptrInt32(replicas)
	return r.Update(ctx, updated)
}

func (r *PirinDBClusterReconciler) patchStatus(ctx context.Context, cluster, base *pirindbv1alpha1.PirinDBCluster) error {
	if cluster == nil || base == nil {
		return nil
	}
	if equality.Semantic.DeepEqual(base.Status, cluster.Status) {
		return nil
	}
	return r.Status().Patch(ctx, cluster, client.MergeFrom(base))
}

func (r *PirinDBClusterReconciler) syncRuntimeStatus(cluster *pirindbv1alpha1.PirinDBCluster, runtimeStatus *clusterAPIStatusResponse) {
	if runtimeStatus == nil {
		return
	}
	cluster.Status.TopologyHash = runtimeStatus.ConfiguredTopologyHash
	cluster.Status.RuntimeTopologyHash = runtimeStatus.RuntimeTopologyHash
	cluster.Status.RuntimeEpoch = runtimeStatus.Epoch
	cluster.Status.RuntimeConditions = make([]pirindbv1alpha1.PirinDBClusterRuntimeCondition, 0, len(runtimeStatus.Conditions))
	for _, condition := range runtimeStatus.Conditions {
		cluster.Status.RuntimeConditions = append(cluster.Status.RuntimeConditions, pirindbv1alpha1.PirinDBClusterRuntimeCondition{
			Type:    condition.Type,
			Status:  condition.Status,
			Reason:  condition.Reason,
			Message: condition.Message,
		})
	}
	cluster.Status.RuntimeNodes = make([]pirindbv1alpha1.PirinDBClusterRuntimeNodeStatus, 0, len(runtimeStatus.NodeStatuses))
	for _, node := range runtimeStatus.NodeStatuses {
		cluster.Status.RuntimeNodes = append(cluster.Status.RuntimeNodes, pirindbv1alpha1.PirinDBClusterRuntimeNodeStatus{
			NodeID:               node.NodeID,
			RedisAddress:         node.RedisAddress,
			HTTPAddress:          node.HTTPAddress,
			OwnedSlots:           node.OwnedSlots,
			TargetSlots:          node.TargetSlots,
			ConfiguredInTopology: node.ConfiguredInTopology,
			Draining:             node.Draining,
			Receiving:            node.Receiving,
		})
	}
}

func (r *PirinDBClusterReconciler) setClusterAPICondition(cluster *pirindbv1alpha1.PirinDBCluster, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               pirinDBClusterConditionClusterAPIReady,
		Status:             status,
		ObservedGeneration: cluster.Generation,
		Reason:             reason,
		Message:            message,
	})
}

func (r *PirinDBClusterReconciler) setProgressingCondition(cluster *pirindbv1alpha1.PirinDBCluster, reason, message string) {
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               pirinDBClusterConditionProgressing,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: cluster.Generation,
		Reason:             reason,
		Message:            message,
	})
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               pirinDBClusterConditionDegraded,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: cluster.Generation,
		Reason:             "AsExpected",
		Message:            "cluster is progressing toward the desired state",
	})
}

func (r *PirinDBClusterReconciler) setDegradedCondition(cluster *pirindbv1alpha1.PirinDBCluster, reason, message string) {
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:               pirinDBClusterConditionDegraded,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: cluster.Generation,
		Reason:             reason,
		Message:            message,
	})
}

func (r *PirinDBClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pirindbv1alpha1.PirinDBCluster{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}

func clusterDesiredReplicas(cluster *pirindbv1alpha1.PirinDBCluster) int32 {
	if cluster.Spec.Replicas == nil || *cluster.Spec.Replicas < 1 {
		return pirinDBDefaultReplicas
	}
	return *cluster.Spec.Replicas
}

func clusterHTTPPort(cluster *pirindbv1alpha1.PirinDBCluster) int32 {
	if cluster.Spec.HTTPPort > 0 {
		return cluster.Spec.HTTPPort
	}
	return pirinDBDefaultHTTPPort
}

func clusterRedisPort(cluster *pirindbv1alpha1.PirinDBCluster) int32 {
	if cluster.Spec.RedisPort > 0 {
		return cluster.Spec.RedisPort
	}
	return pirinDBDefaultRedisPort
}

func clusterSlotCount(cluster *pirindbv1alpha1.PirinDBCluster) int32 {
	if cluster.Spec.SlotCount > 0 {
		return cluster.Spec.SlotCount
	}
	return pirinDBDefaultSlotCount
}

func clusterSyncPolicy(cluster *pirindbv1alpha1.PirinDBCluster) string {
	if strings.TrimSpace(cluster.Spec.SyncPolicy) != "" {
		return cluster.Spec.SyncPolicy
	}
	return "strict"
}

func clusterCheckpointTxThreshold(cluster *pirindbv1alpha1.PirinDBCluster) int32 {
	if cluster.Spec.CheckpointTxThreshold > 0 {
		return cluster.Spec.CheckpointTxThreshold
	}
	return 64
}

func clusterGroupCommitTxThreshold(cluster *pirindbv1alpha1.PirinDBCluster) int32 {
	if cluster.Spec.GroupCommitTxThreshold > 0 {
		return cluster.Spec.GroupCommitTxThreshold
	}
	return 16
}

func clusterGroupCommitWindowMs(cluster *pirindbv1alpha1.PirinDBCluster) int32 {
	if cluster.Spec.GroupCommitWindowMs >= 0 {
		if cluster.Spec.GroupCommitWindowMs > 0 {
			return cluster.Spec.GroupCommitWindowMs
		}
	}
	return 1
}

func clusterLogLevel(cluster *pirindbv1alpha1.PirinDBCluster) string {
	if strings.TrimSpace(cluster.Spec.LogLevel) != "" {
		return cluster.Spec.LogLevel
	}
	return "INFO"
}

func clusterImagePullPolicy(cluster *pirindbv1alpha1.PirinDBCluster) corev1.PullPolicy {
	if cluster.Spec.ImagePullPolicy != "" {
		return cluster.Spec.ImagePullPolicy
	}
	return corev1.PullIfNotPresent
}

func clusterAutoRebalanceOnScaleUp(cluster *pirindbv1alpha1.PirinDBCluster) bool {
	if cluster.Spec.AutoRebalanceOnScaleUp == nil {
		return true
	}
	return *cluster.Spec.AutoRebalanceOnScaleUp
}

func clusterAutoRebalanceOnScaleDown(cluster *pirindbv1alpha1.PirinDBCluster) bool {
	if cluster.Spec.AutoRebalanceOnScaleDown == nil {
		return true
	}
	return *cluster.Spec.AutoRebalanceOnScaleDown
}

func clusterLabels(cluster *pirindbv1alpha1.PirinDBCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     "pirindb",
		"app.kubernetes.io/instance": cluster.Name,
		"pirindb.timson.dev/cluster": cluster.Name,
	}
}

func topologyConfigMapName(cluster *pirindbv1alpha1.PirinDBCluster) string {
	return cluster.Name + "-topology"
}

func headlessServiceName(cluster *pirindbv1alpha1.PirinDBCluster) string {
	return cluster.Name + "-headless"
}

func statefulSetNodeID(base string, ordinal int32) string {
	return fmt.Sprintf("%s-%d", base, ordinal)
}

func statefulSetPodDNS(cluster *pirindbv1alpha1.PirinDBCluster, ordinal int32) string {
	return fmt.Sprintf("%s.%s.%s.svc.cluster.local", statefulSetNodeID(cluster.Name, ordinal), headlessServiceName(cluster), cluster.Namespace)
}

func renderClusterTopology(cluster *pirindbv1alpha1.PirinDBCluster, replicas int32) string {
	var builder strings.Builder
	builder.WriteString("[cluster]\n")
	builder.WriteString(fmt.Sprintf("name = %q\n", fmt.Sprintf("%s/%s", cluster.Namespace, cluster.Name)))
	builder.WriteString(fmt.Sprintf("slot_count = %d\n\n", clusterSlotCount(cluster)))
	for ordinal := int32(0); ordinal < replicas; ordinal++ {
		nodeID := statefulSetNodeID(cluster.Name, ordinal)
		host := statefulSetPodDNS(cluster, ordinal)
		builder.WriteString("[[cluster.nodes]]\n")
		builder.WriteString(fmt.Sprintf("id = %q\n", nodeID))
		builder.WriteString(fmt.Sprintf("redis_address = %q\n", fmt.Sprintf("%s:%d", host, clusterRedisPort(cluster))))
		builder.WriteString(fmt.Sprintf("http_address = %q\n\n", fmt.Sprintf("http://%s:%d", host, clusterHTTPPort(cluster))))
	}
	for ordinal, slotRange := range evenSlotRanges(int(clusterSlotCount(cluster)), int(replicas)) {
		builder.WriteString("[[cluster.bootstrap_slots]]\n")
		builder.WriteString(fmt.Sprintf("node_id = %q\n", statefulSetNodeID(cluster.Name, int32(ordinal))))
		builder.WriteString(fmt.Sprintf("slots = [%q]\n\n", slotRange))
	}
	return builder.String()
}

func evenSlotRanges(slotCount, replicas int) []string {
	if replicas <= 0 {
		return nil
	}
	ranges := make([]string, 0, replicas)
	base := slotCount / replicas
	remainder := slotCount % replicas
	start := 0
	for i := 0; i < replicas; i++ {
		width := base
		if i < remainder {
			width++
		}
		end := start + width - 1
		ranges = append(ranges, fmt.Sprintf("%d-%d", start, end))
		start = end + 1
	}
	return ranges
}

func buildPirinDBArgs(cluster *pirindbv1alpha1.PirinDBCluster) []string {
	return []string{
		fmt.Sprintf("--host=%s", "0.0.0.0"),
		fmt.Sprintf("--port=%d", clusterHTTPPort(cluster)),
		fmt.Sprintf("--redis-enabled=%t", true),
		fmt.Sprintf("--redis-host=%s", "0.0.0.0"),
		fmt.Sprintf("--redis-port=%d", clusterRedisPort(cluster)),
		fmt.Sprintf("--db=%s", "/var/lib/pirindb/pirin.db"),
		fmt.Sprintf("--log=%s", clusterLogLevel(cluster)),
		fmt.Sprintf("--sync-policy=%s", clusterSyncPolicy(cluster)),
		fmt.Sprintf("--checkpoint-tx-threshold=%d", clusterCheckpointTxThreshold(cluster)),
		fmt.Sprintf("--group-commit-tx-threshold=%d", clusterGroupCommitTxThreshold(cluster)),
		fmt.Sprintf("--group-commit-window-ms=%d", clusterGroupCommitWindowMs(cluster)),
		fmt.Sprintf("--cluster-enabled=%t", true),
		fmt.Sprintf("--cluster-slot-count=%d", clusterSlotCount(cluster)),
		fmt.Sprintf("--cluster-topology-file=%s", "/etc/pirindb/topology.toml"),
		fmt.Sprintf("--cluster-require-auth=%t", true),
	}
}

type clusterAdminBearerTokenSetter interface {
	SetBearerToken(token string)
}

func (r *PirinDBClusterReconciler) configureClusterAdminAuth(ctx context.Context, cluster *pirindbv1alpha1.PirinDBCluster) error {
	if cluster.Spec.AdminSecretRef == nil {
		return errors.New("spec.adminSecretRef is required for operator-managed clusters")
	}
	setter, ok := r.ClusterAdmin.(clusterAdminBearerTokenSetter)
	if !ok {
		return nil
	}
	selector := cluster.Spec.AdminSecretRef
	if strings.TrimSpace(selector.Name) == "" || strings.TrimSpace(selector.Key) == "" {
		return errors.New("adminSecretRef name and key are required")
	}
	var secret corev1.Secret
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	if err := reader.Get(ctx, types.NamespacedName{Name: selector.Name, Namespace: cluster.Namespace}, &secret); err != nil {
		return err
	}
	token := strings.TrimSpace(string(secret.Data[selector.Key]))
	if token == "" {
		return fmt.Errorf("secret %s key %s is empty", selector.Name, selector.Key)
	}
	setter.SetBearerToken(token)
	return nil
}

func buildDataPVC(cluster *pirindbv1alpha1.PirinDBCluster) (corev1.PersistentVolumeClaim, error) {
	size := strings.TrimSpace(cluster.Spec.Storage.Size)
	if size == "" {
		size = pirinDBDefaultStorageSize
	}
	quantity, err := resource.ParseQuantity(size)
	if err != nil || quantity.Sign() <= 0 {
		return corev1.PersistentVolumeClaim{}, fmt.Errorf("storage size %q is not a positive Kubernetes quantity", size)
	}
	pvc := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: quantity,
				},
			},
		},
	}
	if cluster.Spec.Storage.StorageClassName != nil {
		pvc.Spec.StorageClassName = cluster.Spec.Storage.StorageClassName
	}
	return pvc, nil
}

func defaultClusterBaseURL(cluster *pirindbv1alpha1.PirinDBCluster) string {
	return defaultClusterNodeBaseURL(cluster, 0)
}

func defaultClusterNodeBaseURL(cluster *pirindbv1alpha1.PirinDBCluster, ordinal int32) string {
	return fmt.Sprintf("http://%s:%d", statefulSetPodDNS(cluster, ordinal), clusterHTTPPort(cluster))
}

// reconcileTopologyAcrossNodes waits for the projected topology file to be
// visible on every expected pod before allowing any pod to reconcile it. This
// preflight is important: reconciling a stale file could otherwise roll back a
// membership change. Once every configured hash agrees, each pod reloads the
// file and must report the same runtime hash, epoch, and cluster identity.
func (r *PirinDBClusterReconciler) reconcileTopologyAcrossNodes(
	ctx context.Context,
	cluster *pirindbv1alpha1.PirinDBCluster,
	replicas int32,
) (*clusterAPIStatusResponse, bool, error) {
	if cluster == nil || replicas < 1 {
		return nil, false, errors.New("cannot reconcile topology without an expected cluster membership")
	}
	resolveNodeBaseURL := r.ResolveNodeBaseURL
	if resolveNodeBaseURL == nil {
		resolveNodeBaseURL = defaultClusterNodeBaseURL
	}

	desiredHash := ""
	var coordinatorStatus *clusterAPIStatusResponse
	for ordinal := int32(0); ordinal < replicas; ordinal++ {
		status, err := r.ClusterAdmin.GetStatus(ctx, resolveNodeBaseURL(cluster, ordinal))
		if err != nil {
			return coordinatorStatus, false, fmt.Errorf("get topology status from %s: %w", statefulSetNodeID(cluster.Name, ordinal), err)
		}
		if ordinal == 0 {
			coordinatorStatus = status
		}
		if !configuredTopologyMatchesReplicas(cluster, status, replicas) {
			return coordinatorStatus, false, nil
		}
		if status.ConfiguredTopologyHash != "" {
			if desiredHash == "" {
				desiredHash = status.ConfiguredTopologyHash
			} else if desiredHash != status.ConfiguredTopologyHash {
				return coordinatorStatus, false, nil
			}
		}
	}

	clusterID := ""
	runtimeHash := ""
	var runtimeEpoch uint64
	for ordinal := int32(0); ordinal < replicas; ordinal++ {
		status, err := r.ClusterAdmin.ReconcileTopology(ctx, resolveNodeBaseURL(cluster, ordinal))
		if err != nil {
			return coordinatorStatus, false, fmt.Errorf("reconcile topology on %s: %w", statefulSetNodeID(cluster.Name, ordinal), err)
		}
		if ordinal == 0 {
			coordinatorStatus = status
			clusterID = status.ClusterID
			runtimeHash = status.RuntimeTopologyHash
			runtimeEpoch = status.Epoch
		}
		if !configuredTopologyMatchesReplicas(cluster, status, replicas) || !runtimeConditionTrue(status.Conditions, "TopologyAligned") {
			return coordinatorStatus, false, nil
		}
		if desiredHash != "" && (status.ConfiguredTopologyHash != desiredHash || status.RuntimeTopologyHash != desiredHash) {
			return coordinatorStatus, false, nil
		}
		if clusterID != "" && status.ClusterID != "" && status.ClusterID != clusterID {
			return coordinatorStatus, false, nil
		}
		if runtimeHash != "" && status.RuntimeTopologyHash != "" && status.RuntimeTopologyHash != runtimeHash {
			return coordinatorStatus, false, nil
		}
		if runtimeEpoch != 0 && status.Epoch != 0 && status.Epoch != runtimeEpoch {
			return coordinatorStatus, false, nil
		}
	}
	return coordinatorStatus, true, nil
}

func findRuntimeNodeStatus(nodes []pirindbv1alpha1.PirinDBClusterRuntimeNodeStatus, nodeID string) (pirindbv1alpha1.PirinDBClusterRuntimeNodeStatus, bool) {
	for _, node := range nodes {
		if node.NodeID == nodeID {
			return node, true
		}
	}
	return pirindbv1alpha1.PirinDBClusterRuntimeNodeStatus{}, false
}

func configuredTopologyMatchesReplicas(cluster *pirindbv1alpha1.PirinDBCluster, status *clusterAPIStatusResponse, replicas int32) bool {
	if cluster == nil || status == nil || replicas < 1 {
		return false
	}
	expectedName := fmt.Sprintf("%s/%s", cluster.Namespace, cluster.Name)
	if status.ConfiguredTopologyName != "" && status.ConfiguredTopologyName != expectedName {
		return false
	}
	expected := make([]string, 0, replicas)
	for ordinal := int32(0); ordinal < replicas; ordinal++ {
		expected = append(expected, statefulSetNodeID(cluster.Name, ordinal))
	}
	actual := append([]string(nil), status.ConfiguredNodeIDs...)
	if len(actual) == 0 {
		if !runtimeConditionTrue(status.Conditions, "TopologyAligned") || len(status.NodeStatuses) != int(replicas) {
			return false
		}
		for _, node := range status.NodeStatuses {
			actual = append(actual, node.NodeID)
		}
	}
	sort.Strings(expected)
	sort.Strings(actual)
	if len(expected) != len(actual) {
		return false
	}
	for index := range expected {
		if expected[index] != actual[index] {
			return false
		}
	}
	return true
}

func clusterOperationIdempotencyKey(cluster *pirindbv1alpha1.PirinDBCluster, kind, nodeID string) string {
	identity := string(cluster.UID)
	if identity == "" {
		identity = cluster.Namespace + "/" + cluster.Name
	}
	return fmt.Sprintf("%s:%d:%s:%s", identity, cluster.Generation, kind, nodeID)
}

func pickScaleOutDestinationNode(nodes []clusterAPINodeStatus) string {
	if len(nodes) == 0 {
		return ""
	}
	sorted := append([]clusterAPINodeStatus(nil), nodes...)
	sort.Slice(sorted, func(i, j int) bool {
		leftDeficit := sorted[i].TargetSlots - sorted[i].OwnedSlots
		rightDeficit := sorted[j].TargetSlots - sorted[j].OwnedSlots
		if leftDeficit == rightDeficit {
			return sorted[i].NodeID > sorted[j].NodeID
		}
		return leftDeficit > rightDeficit
	})
	for _, node := range sorted {
		if node.TargetSlots > node.OwnedSlots {
			return node.NodeID
		}
	}
	return ""
}

func conditionStatus(value bool) metav1.ConditionStatus {
	if value {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

func resourcesReadyReason(ready bool) string {
	if ready {
		return "AllPodsReady"
	}
	return "PodsPending"
}

func runtimeConditionTrue(conditions []clusterAPICondition, conditionType string) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType {
			return strings.EqualFold(condition.Status, "true")
		}
	}
	return false
}

func runtimeConditionReason(conditions []clusterAPICondition, conditionType, fallback string) string {
	for _, condition := range conditions {
		if condition.Type == conditionType && condition.Reason != "" {
			return condition.Reason
		}
	}
	return fallback
}

func runtimeConditionMessage(conditions []clusterAPICondition, conditionType, fallback string) string {
	for _, condition := range conditions {
		if condition.Type == conditionType && condition.Message != "" {
			return condition.Message
		}
	}
	return fallback
}

func ptrInt32(value int32) *int32 {
	return &value
}

func ptrInt64(value int64) *int64 {
	return &value
}

func ptrBool(value bool) *bool {
	return &value
}

func marshalJSON(v any) string {
	raw, _ := json.Marshal(v)
	return string(raw)
}
