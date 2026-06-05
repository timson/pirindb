package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/timson/pirindb/storage"
)

const (
	clusterStateBucketNameConst = "__pirin_cluster_meta__"
	clusterStateKeyNameConst    = "state"

	clusterMoveStageCopying = "copying"
	clusterMoveStageCatchup = "catchup"
	clusterMoveStageAsking  = "asking"
)

type clusterNodeState struct {
	ID           string `json:"id"`
	RedisAddress string `json:"redis_address"`
	HTTPAddress  string `json:"http_address"`
}

type clusterPendingMove struct {
	ID                string `json:"id,omitempty"`
	SourceNodeID      string `json:"source_node_id"`
	DestinationNodeID string `json:"destination_node_id"`
	StartSlot         int    `json:"start_slot"`
	EndSlot           int    `json:"end_slot"`
	Stage             string `json:"stage,omitempty"`
}

type clusterState struct {
	ClusterID    string              `json:"cluster_id"`
	Version      uint64              `json:"version"`
	Epoch        uint64              `json:"epoch"`
	TopologyName string              `json:"topology_name,omitempty"`
	TopologyHash string              `json:"topology_hash,omitempty"`
	SlotCount    int                 `json:"slot_count"`
	Nodes        []clusterNodeState  `json:"nodes"`
	SlotOwners   []string            `json:"slot_owners"`
	Rebalancing  bool                `json:"rebalancing"`
	PendingMove  *clusterPendingMove `json:"pending_move,omitempty"`
}

type clusterSlotRange struct {
	StartSlot int              `json:"start_slot"`
	EndSlot   int              `json:"end_slot"`
	Node      clusterNodeState `json:"node"`
}

type clusterRoute struct {
	slot   int
	owner  clusterNodeState
	hasKey bool
}

type ClusterManager struct {
	DB          *storage.DB
	Logger      *slog.Logger
	LocalNodeID string
	Topology    *ClusterTopologyConfig

	mu    sync.RWMutex
	state *clusterState
}

func validateClusterConfig(cfg *Config) error {
	if cfg == nil || cfg.Cluster == nil || !cfg.Cluster.Enabled {
		return nil
	}
	if cfg.Cluster.NodeID == "" {
		return errors.New("cluster.node_id is required when cluster mode is enabled")
	}
	topology, err := effectiveClusterTopology(cfg.Cluster)
	if err != nil {
		return err
	}
	if topology.SlotCount <= 0 {
		return errors.New("cluster.slot_count must be greater than zero")
	}
	if len(topology.Nodes) == 0 {
		return errors.New("cluster.nodes must contain at least one node when cluster mode is enabled")
	}

	seenNodeIDs := make(map[string]struct{}, len(topology.Nodes))
	localNodeFound := false
	for _, node := range topology.Nodes {
		if node == nil {
			return errors.New("cluster.nodes cannot contain null entries")
		}
		if node.ID == "" {
			return errors.New("cluster node id is required")
		}
		if node.RedisAddress == "" {
			return fmt.Errorf("cluster node %q redis_address is required", node.ID)
		}
		if node.HTTPAddress == "" {
			return fmt.Errorf("cluster node %q http_address is required", node.ID)
		}
		if _, exists := seenNodeIDs[node.ID]; exists {
			return fmt.Errorf("cluster node id %q is duplicated", node.ID)
		}
		seenNodeIDs[node.ID] = struct{}{}
		if node.ID == cfg.Cluster.NodeID {
			localNodeFound = true
		}
	}
	if !localNodeFound {
		return fmt.Errorf("cluster.node_id %q does not exist in cluster.nodes", cfg.Cluster.NodeID)
	}
	requireFullCoverage := cfg.Cluster.Topology == nil || len(resolveTopologyBootstrapAssignments(topology)) > 0
	if err := validateTopologyBootstrapAssignments(topology, requireFullCoverage); err != nil {
		return err
	}
	return nil
}

func NewClusterManager(cfg *Config, db *storage.DB, logger *slog.Logger) (*ClusterManager, error) {
	if cfg == nil || cfg.Cluster == nil || !cfg.Cluster.Enabled {
		return nil, nil
	}
	manager := &ClusterManager{
		DB:          db,
		Logger:      logger,
		LocalNodeID: cfg.Cluster.NodeID,
		Topology:    cloneClusterTopology(cfg.Cluster.Topology),
	}
	if err := manager.loadOrInitialize(cfg.Cluster); err != nil {
		return nil, err
	}
	return manager, nil
}

func effectiveClusterTopology(cfg *ClusterConfig) (*ClusterTopologyConfig, error) {
	if cfg == nil {
		return nil, errors.New("missing cluster config")
	}
	if cfg.Topology != nil {
		return cloneClusterTopology(cfg.Topology), nil
	}
	topology := &ClusterTopologyConfig{
		SlotCount: cfg.SlotCount,
		Nodes:     make([]*ClusterNodeConfig, 0, len(cfg.Nodes)),
	}
	for _, node := range cfg.Nodes {
		if node == nil {
			continue
		}
		clonedNode := &ClusterNodeConfig{
			ID:           node.ID,
			RedisAddress: node.RedisAddress,
			HTTPAddress:  node.HTTPAddress,
			Slots:        append([]string(nil), node.Slots...),
		}
		topology.Nodes = append(topology.Nodes, clonedNode)
		if len(node.Slots) > 0 {
			topology.BootstrapSlots = append(topology.BootstrapSlots, &ClusterBootstrapSlotsConfig{
				NodeID: node.ID,
				Slots:  append([]string(nil), node.Slots...),
			})
		}
	}
	return topology, nil
}

func cloneClusterTopology(topology *ClusterTopologyConfig) *ClusterTopologyConfig {
	if topology == nil {
		return nil
	}
	cloned := &ClusterTopologyConfig{
		Name:      topology.Name,
		SlotCount: topology.SlotCount,
		Nodes:     make([]*ClusterNodeConfig, 0, len(topology.Nodes)),
	}
	for _, node := range topology.Nodes {
		if node == nil {
			cloned.Nodes = append(cloned.Nodes, nil)
			continue
		}
		cloned.Nodes = append(cloned.Nodes, &ClusterNodeConfig{
			ID:           node.ID,
			RedisAddress: node.RedisAddress,
			HTTPAddress:  node.HTTPAddress,
			Slots:        append([]string(nil), node.Slots...),
		})
	}
	cloned.BootstrapSlots = make([]*ClusterBootstrapSlotsConfig, 0, len(topology.BootstrapSlots))
	for _, bootstrap := range topology.BootstrapSlots {
		if bootstrap == nil {
			cloned.BootstrapSlots = append(cloned.BootstrapSlots, nil)
			continue
		}
		cloned.BootstrapSlots = append(cloned.BootstrapSlots, &ClusterBootstrapSlotsConfig{
			NodeID: bootstrap.NodeID,
			Slots:  append([]string(nil), bootstrap.Slots...),
		})
	}
	return cloned
}

func resolveTopologyBootstrapAssignments(topology *ClusterTopologyConfig) []*ClusterBootstrapSlotsConfig {
	if topology == nil {
		return nil
	}
	if len(topology.BootstrapSlots) > 0 {
		assignments := make([]*ClusterBootstrapSlotsConfig, 0, len(topology.BootstrapSlots))
		for _, bootstrap := range topology.BootstrapSlots {
			if bootstrap == nil {
				continue
			}
			assignments = append(assignments, bootstrap)
		}
		return assignments
	}
	assignments := make([]*ClusterBootstrapSlotsConfig, 0, len(topology.Nodes))
	for _, node := range topology.Nodes {
		if node == nil || len(node.Slots) == 0 {
			continue
		}
		assignments = append(assignments, &ClusterBootstrapSlotsConfig{
			NodeID: node.ID,
			Slots:  append([]string(nil), node.Slots...),
		})
	}
	return assignments
}

func validateTopologyBootstrapAssignments(topology *ClusterTopologyConfig, requireFullCoverage bool) error {
	if topology == nil {
		return errors.New("missing cluster topology")
	}
	assignments := resolveTopologyBootstrapAssignments(topology)
	if len(assignments) == 0 {
		if requireFullCoverage {
			return errors.New("cluster bootstrap slots are required to initialize slot ownership")
		}
		return nil
	}
	seenNodes := make(map[string]struct{}, len(topology.Nodes))
	for _, node := range topology.Nodes {
		if node != nil {
			seenNodes[node.ID] = struct{}{}
		}
	}
	assignedSlots := make([]bool, topology.SlotCount)
	for _, assignment := range assignments {
		if assignment == nil {
			return errors.New("cluster bootstrap_slots cannot contain null entries")
		}
		if _, ok := seenNodes[assignment.NodeID]; !ok {
			return fmt.Errorf("cluster bootstrap_slots references unknown node %q", assignment.NodeID)
		}
		for _, spec := range assignment.Slots {
			startSlot, endSlot, err := parseClusterSlotSpec(spec, topology.SlotCount)
			if err != nil {
				return fmt.Errorf("cluster bootstrap_slots for node %q invalid slot spec %q: %w", assignment.NodeID, spec, err)
			}
			for slot := startSlot; slot <= endSlot; slot++ {
				if assignedSlots[slot] {
					return fmt.Errorf("cluster slot %d is assigned more than once", slot)
				}
				assignedSlots[slot] = true
			}
		}
	}
	if requireFullCoverage {
		for slot, assigned := range assignedSlots {
			if !assigned {
				return fmt.Errorf("cluster slot %d is unassigned", slot)
			}
		}
	}
	return nil
}

func parseClusterSlotSpec(spec string, slotCount int) (int, int, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return 0, 0, errors.New("empty slot spec")
	}
	if strings.Contains(spec, "-") {
		parts := strings.SplitN(spec, "-", 2)
		startSlot, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return 0, 0, errors.New("invalid start slot")
		}
		endSlot, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			return 0, 0, errors.New("invalid end slot")
		}
		if startSlot < 0 || endSlot < 0 || startSlot > endSlot || endSlot >= slotCount {
			return 0, 0, errors.New("slot range is out of bounds")
		}
		return startSlot, endSlot, nil
	}
	slot, err := strconv.Atoi(spec)
	if err != nil {
		return 0, 0, errors.New("invalid slot")
	}
	if slot < 0 || slot >= slotCount {
		return 0, 0, errors.New("slot is out of bounds")
	}
	return slot, slot, nil
}

func buildClusterStateFromConfig(cfg *ClusterConfig) (*clusterState, error) {
	topology, err := effectiveClusterTopology(cfg)
	if err != nil {
		return nil, err
	}
	return buildClusterStateFromTopology(topology)
}

func buildClusterMembershipFromTopology(topology *ClusterTopologyConfig) ([]clusterNodeState, error) {
	if topology == nil {
		return nil, errors.New("missing cluster topology")
	}
	nodes := make([]clusterNodeState, 0, len(topology.Nodes))
	for _, nodeCfg := range topology.Nodes {
		if nodeCfg == nil {
			return nil, errors.New("cluster topology nodes cannot contain null entries")
		}
		nodes = append(nodes, clusterNodeState{
			ID:           nodeCfg.ID,
			RedisAddress: nodeCfg.RedisAddress,
			HTTPAddress:  nodeCfg.HTTPAddress,
		})
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	return nodes, nil
}

func buildInitialSlotOwnersFromTopology(topology *ClusterTopologyConfig) ([]string, error) {
	if topology == nil {
		return nil, errors.New("missing cluster topology")
	}
	if err := validateTopologyBootstrapAssignments(topology, true); err != nil {
		return nil, err
	}
	owners := make([]string, topology.SlotCount)
	for _, assignment := range resolveTopologyBootstrapAssignments(topology) {
		for _, spec := range assignment.Slots {
			startSlot, endSlot, err := parseClusterSlotSpec(spec, topology.SlotCount)
			if err != nil {
				return nil, err
			}
			for slot := startSlot; slot <= endSlot; slot++ {
				owners[slot] = assignment.NodeID
			}
		}
	}
	return owners, nil
}

func clusterTopologyHash(topology *ClusterTopologyConfig) string {
	if topology == nil {
		return ""
	}
	payload := make([]byte, 0, 256)
	payload = append(payload, topology.Name...)
	payload = append(payload, '|')
	payload = append(payload, strconv.Itoa(topology.SlotCount)...)
	payload = append(payload, ';')
	nodes := cloneClusterTopology(topology).Nodes
	sort.Slice(nodes, func(i, j int) bool {
		switch {
		case nodes[i] == nil && nodes[j] == nil:
			return false
		case nodes[i] == nil:
			return false
		case nodes[j] == nil:
			return true
		default:
			return nodes[i].ID < nodes[j].ID
		}
	})
	for _, node := range nodes {
		if node == nil {
			continue
		}
		payload = append(payload, node.ID...)
		payload = append(payload, '@')
		payload = append(payload, node.RedisAddress...)
		payload = append(payload, '|')
		payload = append(payload, node.HTTPAddress...)
		payload = append(payload, ';')
	}
	assignments := resolveTopologyBootstrapAssignments(topology)
	sort.Slice(assignments, func(i, j int) bool {
		return assignments[i].NodeID < assignments[j].NodeID
	})
	for _, assignment := range assignments {
		payload = append(payload, assignment.NodeID...)
		payload = append(payload, ':')
		for _, spec := range assignment.Slots {
			payload = append(payload, spec...)
			payload = append(payload, ',')
		}
		payload = append(payload, ';')
	}
	return fmt.Sprintf("%08x", crc32.ChecksumIEEE(payload))
}

func buildClusterStateFromTopology(topology *ClusterTopologyConfig) (*clusterState, error) {
	if topology == nil {
		return nil, errors.New("missing cluster topology")
	}
	nodes, err := buildClusterMembershipFromTopology(topology)
	if err != nil {
		return nil, err
	}
	owners, err := buildInitialSlotOwnersFromTopology(topology)
	if err != nil {
		return nil, err
	}
	clusterIDInput := make([]byte, 0, len(nodes)*64)
	for _, node := range nodes {
		clusterIDInput = append(clusterIDInput, node.ID...)
		clusterIDInput = append(clusterIDInput, '@')
		clusterIDInput = append(clusterIDInput, node.RedisAddress...)
		clusterIDInput = append(clusterIDInput, '|')
		clusterIDInput = append(clusterIDInput, node.HTTPAddress...)
		clusterIDInput = append(clusterIDInput, ';')
	}
	clusterID := fmt.Sprintf("pirin-%08x", crc32.ChecksumIEEE(clusterIDInput))
	return &clusterState{
		ClusterID:    clusterID,
		Version:      1,
		Epoch:        1,
		TopologyName: topology.Name,
		TopologyHash: clusterTopologyHash(topology),
		SlotCount:    topology.SlotCount,
		Nodes:        nodes,
		SlotOwners:   owners,
	}, nil
}

func encodeClusterState(state *clusterState) ([]byte, error) {
	return json.Marshal(state)
}

func decodeClusterState(raw []byte) (*clusterState, error) {
	var state clusterState
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, err
	}
	return &state, validateClusterState(state.Clone())
}

func reconcileMembershipFromTopology(existingState *clusterState, topology *ClusterTopologyConfig) (*clusterState, bool, error) {
	if existingState == nil {
		return nil, false, errors.New("missing cluster state")
	}
	if topology == nil {
		return existingState.Clone(), false, nil
	}
	if topology.SlotCount > 0 && existingState.SlotCount != topology.SlotCount {
		return nil, false, fmt.Errorf("runtime slot_count=%d does not match topology slot_count=%d", existingState.SlotCount, topology.SlotCount)
	}
	desiredNodes, err := buildClusterMembershipFromTopology(topology)
	if err != nil {
		return nil, false, err
	}
	desiredByID := make(map[string]clusterNodeState, len(desiredNodes))
	for _, node := range desiredNodes {
		desiredByID[node.ID] = node
	}
	next := existingState.Clone()
	changed := false

	if next.TopologyName != topology.Name {
		next.TopologyName = topology.Name
		changed = true
	}
	topologyHash := clusterTopologyHash(topology)
	if next.TopologyHash != topologyHash {
		next.TopologyHash = topologyHash
		changed = true
	}

	filteredNodes := make([]clusterNodeState, 0, len(desiredNodes))
	for _, existing := range next.Nodes {
		if desired, ok := desiredByID[existing.ID]; ok {
			if existing.RedisAddress != desired.RedisAddress || existing.HTTPAddress != desired.HTTPAddress {
				existing.RedisAddress = desired.RedisAddress
				existing.HTTPAddress = desired.HTTPAddress
				changed = true
			}
			filteredNodes = append(filteredNodes, existing)
			delete(desiredByID, existing.ID)
			continue
		}
		if containsString(next.SlotOwners, existing.ID) {
			return nil, false, fmt.Errorf("cannot remove node %q from topology while it still owns slots", existing.ID)
		}
		if next.PendingMove != nil && (next.PendingMove.SourceNodeID == existing.ID || next.PendingMove.DestinationNodeID == existing.ID) {
			return nil, false, fmt.Errorf("cannot remove node %q from topology while it participates in a pending move", existing.ID)
		}
		changed = true
	}
	for _, node := range desiredByID {
		filteredNodes = append(filteredNodes, node)
		changed = true
	}
	sort.Slice(filteredNodes, func(i, j int) bool {
		return filteredNodes[i].ID < filteredNodes[j].ID
	})
	next.Nodes = filteredNodes
	if changed {
		next.Version++
		if next.Epoch == 0 {
			next.Epoch = 1
		}
		next.Epoch++
	}
	if err := validateClusterState(next); err != nil {
		return nil, false, err
	}
	return next, changed, nil
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func validateClusterState(state *clusterState) error {
	if state == nil {
		return errors.New("missing cluster state")
	}
	if state.ClusterID == "" {
		return errors.New("cluster id is required")
	}
	if state.SlotCount <= 0 {
		return errors.New("slot count must be greater than zero")
	}
	if len(state.SlotOwners) != state.SlotCount {
		return errors.New("slot owner table length does not match slot count")
	}
	if len(state.Nodes) == 0 {
		return errors.New("cluster must contain at least one node")
	}
	nodeByID := make(map[string]clusterNodeState, len(state.Nodes))
	for _, node := range state.Nodes {
		if node.ID == "" || node.RedisAddress == "" || node.HTTPAddress == "" {
			return errors.New("cluster nodes must include id, redis address, and http address")
		}
		nodeByID[node.ID] = node
	}
	for slot, ownerID := range state.SlotOwners {
		if ownerID == "" {
			return fmt.Errorf("cluster slot %d has no owner", slot)
		}
		if _, ok := nodeByID[ownerID]; !ok {
			return fmt.Errorf("cluster slot %d references unknown owner %q", slot, ownerID)
		}
	}
	if state.PendingMove != nil {
		if state.PendingMove.StartSlot < 0 || state.PendingMove.EndSlot < state.PendingMove.StartSlot || state.PendingMove.EndSlot >= state.SlotCount {
			return errors.New("pending move slot range is invalid")
		}
		switch state.PendingMove.Stage {
		case "", clusterMoveStageCopying, clusterMoveStageCatchup, clusterMoveStageAsking:
		default:
			return errors.New("pending move stage is invalid")
		}
		if _, ok := nodeByID[state.PendingMove.SourceNodeID]; !ok {
			return errors.New("pending move source node does not exist")
		}
		if _, ok := nodeByID[state.PendingMove.DestinationNodeID]; !ok {
			return errors.New("pending move destination node does not exist")
		}
	}
	return nil
}

func clusterStateBucketName() []byte {
	return []byte(clusterStateBucketNameConst)
}

func clusterStateKey() []byte {
	return []byte(clusterStateKeyNameConst)
}

func (manager *ClusterManager) loadOrInitialize(cfg *ClusterConfig) error {
	topology, err := effectiveClusterTopology(cfg)
	if err != nil {
		return err
	}
	manager.Topology = cloneClusterTopology(topology)
	reconcileOnLoad := cfg != nil && (cfg.Topology != nil || strings.TrimSpace(cfg.TopologyFile) != "")
	var loaded *clusterState
	err = manager.DB.Update(func(tx *storage.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(clusterStateBucketName())
		if err != nil {
			return err
		}
		raw, found := bucket.Get(clusterStateKey())
		if found {
			state, decodeErr := decodeClusterState(raw)
			if decodeErr != nil {
				return decodeErr
			}
			if reconcileOnLoad {
				reconciled, changed, reconcileErr := reconcileMembershipFromTopology(state, topology)
				if reconcileErr != nil {
					return reconcileErr
				}
				if changed {
					encoded, encodeErr := encodeClusterState(reconciled)
					if encodeErr != nil {
						return encodeErr
					}
					if err = bucket.Put(clusterStateKey(), encoded); err != nil {
						return err
					}
					loaded = reconciled
					return nil
				}
			}
			loaded = state
			return nil
		}
		state, buildErr := buildClusterStateFromTopology(topology)
		if buildErr != nil {
			return buildErr
		}
		encoded, encodeErr := encodeClusterState(state)
		if encodeErr != nil {
			return encodeErr
		}
		if err = bucket.Put(clusterStateKey(), encoded); err != nil {
			return err
		}
		loaded = state
		return nil
	})
	if err != nil {
		return err
	}
	if _, ok := loaded.NodeByID(manager.LocalNodeID); !ok {
		return fmt.Errorf("cluster.node_id %q does not exist in cluster membership", manager.LocalNodeID)
	}

	manager.mu.Lock()
	manager.state = loaded
	manager.mu.Unlock()
	return nil
}

func (manager *ClusterManager) Enabled() bool {
	return manager != nil
}

func (manager *ClusterManager) Snapshot() *clusterState {
	if manager == nil {
		return nil
	}
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	if manager.state == nil {
		return nil
	}
	return manager.state.Clone()
}

func (state *clusterState) Clone() *clusterState {
	if state == nil {
		return nil
	}
	cloned := &clusterState{
		ClusterID:    state.ClusterID,
		Version:      state.Version,
		Epoch:        state.Epoch,
		TopologyName: state.TopologyName,
		TopologyHash: state.TopologyHash,
		SlotCount:    state.SlotCount,
		Nodes:        append([]clusterNodeState(nil), state.Nodes...),
		SlotOwners:   append([]string(nil), state.SlotOwners...),
		Rebalancing:  state.Rebalancing,
	}
	if state.PendingMove != nil {
		move := *state.PendingMove
		cloned.PendingMove = &move
	}
	return cloned
}

func (manager *ClusterManager) persistState(state *clusterState) error {
	if err := validateClusterState(state); err != nil {
		return err
	}
	encoded, err := encodeClusterState(state)
	if err != nil {
		return err
	}
	if err = manager.DB.Update(func(tx *storage.Tx) error {
		bucket, bucketErr := tx.CreateBucketIfNotExists(clusterStateBucketName())
		if bucketErr != nil {
			return bucketErr
		}
		return bucket.Put(clusterStateKey(), encoded)
	}); err != nil {
		return err
	}
	manager.mu.Lock()
	manager.state = state.Clone()
	manager.mu.Unlock()
	return nil
}

func (manager *ClusterManager) ReplaceState(state *clusterState) error {
	if manager == nil {
		return errors.New("cluster mode is disabled")
	}
	return manager.persistState(state.Clone())
}

func (manager *ClusterManager) ConfiguredTopology() *ClusterTopologyConfig {
	if manager == nil {
		return nil
	}
	return cloneClusterTopology(manager.Topology)
}

func (manager *ClusterManager) ReconcileConfiguredTopology() (*clusterState, error) {
	if manager == nil {
		return nil, errors.New("cluster mode is disabled")
	}
	state := manager.Snapshot()
	if state == nil {
		return nil, errors.New("cluster state is unavailable")
	}
	reconciled, changed, err := reconcileMembershipFromTopology(state, manager.Topology)
	if err != nil {
		return nil, err
	}
	if !changed {
		return reconciled, nil
	}
	if err = manager.persistState(reconciled); err != nil {
		return nil, err
	}
	return reconciled, nil
}

func (manager *ClusterManager) AddNode(node clusterNodeState) (*clusterState, error) {
	if manager == nil {
		return nil, errors.New("cluster mode is disabled")
	}
	if node.ID == "" || node.RedisAddress == "" || node.HTTPAddress == "" {
		return nil, errors.New("cluster node must include id, redis address, and http address")
	}

	state := manager.Snapshot()
	if state == nil {
		return nil, errors.New("cluster state is unavailable")
	}
	for _, existing := range state.Nodes {
		switch {
		case existing.ID == node.ID && existing.RedisAddress == node.RedisAddress && existing.HTTPAddress == node.HTTPAddress:
			return state, nil
		case existing.ID == node.ID:
			return nil, fmt.Errorf("cluster node %q already exists with different addresses", node.ID)
		case existing.RedisAddress == node.RedisAddress:
			return nil, fmt.Errorf("cluster redis address %q is already used by node %q", node.RedisAddress, existing.ID)
		case existing.HTTPAddress == node.HTTPAddress:
			return nil, fmt.Errorf("cluster http address %q is already used by node %q", node.HTTPAddress, existing.ID)
		}
	}

	state.Nodes = append(state.Nodes, node)
	sort.Slice(state.Nodes, func(i, j int) bool {
		return state.Nodes[i].ID < state.Nodes[j].ID
	})
	state.Version++
	if state.Epoch == 0 {
		state.Epoch = 1
	}
	state.Epoch++
	if err := manager.persistState(state); err != nil {
		return nil, err
	}
	return state, nil
}

func (manager *ClusterManager) BeginRebalance(move clusterPendingMove) (*clusterState, error) {
	if manager == nil {
		return nil, errors.New("cluster mode is disabled")
	}
	state := manager.Snapshot()
	if state == nil {
		return nil, errors.New("cluster state is unavailable")
	}
	if state.Rebalancing {
		return nil, errors.New("cluster is already rebalancing")
	}
	if move.StartSlot < 0 || move.EndSlot < move.StartSlot || move.EndSlot >= state.SlotCount {
		return nil, errors.New("slot range is invalid")
	}
	sourceNode, ok := state.NodeByID(move.SourceNodeID)
	if !ok {
		return nil, fmt.Errorf("unknown source node %q", move.SourceNodeID)
	}
	if _, ok = state.NodeByID(move.DestinationNodeID); !ok {
		return nil, fmt.Errorf("unknown destination node %q", move.DestinationNodeID)
	}
	for slot := move.StartSlot; slot <= move.EndSlot; slot++ {
		if state.SlotOwners[slot] != sourceNode.ID {
			return nil, fmt.Errorf("slot %d is owned by %s, not %s", slot, state.SlotOwners[slot], sourceNode.ID)
		}
	}
	state.Rebalancing = true
	state.PendingMove = &clusterPendingMove{
		ID:                move.ID,
		SourceNodeID:      move.SourceNodeID,
		DestinationNodeID: move.DestinationNodeID,
		StartSlot:         move.StartSlot,
		EndSlot:           move.EndSlot,
		Stage:             clusterMoveStageCopying,
	}
	if state.Epoch == 0 {
		state.Epoch = 1
	}
	state.Epoch++
	if err := manager.persistState(state); err != nil {
		return nil, err
	}
	return state, nil
}

func (manager *ClusterManager) SetRebalanceStage(stage string) (*clusterState, error) {
	if manager == nil {
		return nil, errors.New("cluster mode is disabled")
	}
	state := manager.Snapshot()
	if state == nil {
		return nil, errors.New("cluster state is unavailable")
	}
	if !state.Rebalancing || state.PendingMove == nil {
		return nil, errors.New("cluster is not rebalancing")
	}
	switch stage {
	case clusterMoveStageCopying, clusterMoveStageCatchup, clusterMoveStageAsking:
	default:
		return nil, errors.New("unsupported rebalance stage")
	}
	state.PendingMove.Stage = stage
	if state.Epoch == 0 {
		state.Epoch = 1
	}
	state.Epoch++
	if err := manager.persistState(state); err != nil {
		return nil, err
	}
	return state, nil
}

func (manager *ClusterManager) FinishRebalance(move clusterPendingMove) (*clusterState, error) {
	if manager == nil {
		return nil, errors.New("cluster mode is disabled")
	}
	state := manager.Snapshot()
	if state == nil {
		return nil, errors.New("cluster state is unavailable")
	}
	if !state.Rebalancing || state.PendingMove == nil {
		return nil, errors.New("cluster is not rebalancing")
	}
	for slot := move.StartSlot; slot <= move.EndSlot; slot++ {
		state.SlotOwners[slot] = move.DestinationNodeID
	}
	state.Rebalancing = false
	state.PendingMove = nil
	state.Version++
	if state.Epoch == 0 {
		state.Epoch = 1
	}
	state.Epoch++
	if err := manager.persistState(state); err != nil {
		return nil, err
	}
	return state, nil
}

func (manager *ClusterManager) AbortRebalance() (*clusterState, error) {
	if manager == nil {
		return nil, errors.New("cluster mode is disabled")
	}
	state := manager.Snapshot()
	if state == nil {
		return nil, errors.New("cluster state is unavailable")
	}
	state.Rebalancing = false
	state.PendingMove = nil
	if state.Epoch == 0 {
		state.Epoch = 1
	}
	state.Epoch++
	if err := manager.persistState(state); err != nil {
		return nil, err
	}
	return state, nil
}

func (state *clusterState) NodeByID(nodeID string) (clusterNodeState, bool) {
	for _, node := range state.Nodes {
		if node.ID == nodeID {
			return node, true
		}
	}
	return clusterNodeState{}, false
}

func (move *clusterPendingMove) ContainsSlot(slot int) bool {
	if move == nil {
		return false
	}
	return slot >= move.StartSlot && slot <= move.EndSlot
}

func (state *clusterState) PendingMoveForSlot(slot int) *clusterPendingMove {
	if state == nil || state.PendingMove == nil || !state.PendingMove.ContainsSlot(slot) {
		return nil
	}
	return state.PendingMove
}

func (manager *ClusterManager) LocalNode() (clusterNodeState, bool) {
	state := manager.Snapshot()
	if state == nil {
		return clusterNodeState{}, false
	}
	return state.NodeByID(manager.LocalNodeID)
}

func clusterHashKey(key []byte) []byte {
	start := bytes.IndexByte(key, '{')
	if start == -1 {
		return key
	}
	end := bytes.IndexByte(key[start+1:], '}')
	if end <= 0 {
		return key
	}
	return key[start+1 : start+1+end]
}

func clusterCRC16(data []byte) uint16 {
	var crc uint16
	for _, b := range data {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

func ClusterKeySlot(key []byte, slotCount int) int {
	if slotCount <= 0 {
		return 0
	}
	hashKey := clusterHashKey(key)
	return int(clusterCRC16(hashKey) % uint16(slotCount))
}

func (manager *ClusterManager) RouteKeys(keys [][]byte) (*clusterRoute, error) {
	if manager == nil {
		return &clusterRoute{}, nil
	}
	state := manager.Snapshot()
	if state == nil {
		return nil, errors.New("cluster state is unavailable")
	}
	var (
		slot   int
		hasKey bool
	)
	for _, key := range keys {
		currentSlot := ClusterKeySlot(key, state.SlotCount)
		if !hasKey {
			slot = currentSlot
			hasKey = true
			continue
		}
		if slot != currentSlot {
			return nil, errors.New("CROSSSLOT Keys in request don't hash to the same slot")
		}
	}
	if !hasKey {
		return &clusterRoute{}, nil
	}
	ownerID := state.SlotOwners[slot]
	owner, ok := state.NodeByID(ownerID)
	if !ok {
		return nil, fmt.Errorf("cluster owner for slot %d is missing", slot)
	}
	return &clusterRoute{
		slot:   slot,
		owner:  owner,
		hasKey: true,
	}, nil
}

func (manager *ClusterManager) IsLocalOwner(slot int) bool {
	if manager == nil {
		return true
	}
	state := manager.Snapshot()
	if state == nil || slot < 0 || slot >= len(state.SlotOwners) {
		return false
	}
	return state.SlotOwners[slot] == manager.LocalNodeID
}

func (manager *ClusterManager) SlotRanges() []clusterSlotRange {
	state := manager.Snapshot()
	if state == nil || len(state.SlotOwners) == 0 {
		return nil
	}
	ranges := make([]clusterSlotRange, 0)
	startSlot := 0
	currentOwner := state.SlotOwners[0]
	for slot := 1; slot <= len(state.SlotOwners); slot++ {
		if slot < len(state.SlotOwners) && state.SlotOwners[slot] == currentOwner {
			continue
		}
		node, _ := state.NodeByID(currentOwner)
		ranges = append(ranges, clusterSlotRange{
			StartSlot: startSlot,
			EndSlot:   slot - 1,
			Node:      node,
		})
		if slot < len(state.SlotOwners) {
			startSlot = slot
			currentOwner = state.SlotOwners[slot]
		}
	}
	return ranges
}
