package main

type ClusterStatusResponse struct {
	Shards map[string]*ShardStatusResponse `json:"shards"`
	Status string                          `json:"status"`
}

type ShardStatusResponse struct {
	Name             string `json:"name"`
	Status           string `json:"status"`
	Host             string `json:"host"`
	Port             int    `json:"port"`
	GossipPort       int    `json:"gossipport"`
	Scheme           string `json:"scheme"`
	CurrentRingHash  string `json:"currentRingHash"`
	PreviousRingHash string `json:"previousRingHash"`
}
