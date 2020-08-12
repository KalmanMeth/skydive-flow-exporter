/*
 * Copyright (C) 2019 IBM, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy ofthe License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specificlanguage governing permissions and
 * limitations under the License.
 *
 */

/*
 * This is a skeleton program to export skydive flow information to prometheus.
 * For each captured flow, we export the total number of bytes transferred on that flow.
 * Flows that have been inactive for some time are removed from the report.
 * Users may use the enclosed example as a base upon which to report additional skydive metrics through prometheus.
 */

package pkg

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	cache "github.com/pmylund/go-cache"
	"github.com/spf13/viper"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/skydive-project/skydive-flow-exporter/core"
	"github.com/skydive-project/skydive/flow"
	"github.com/skydive-project/skydive/logging"
)

// Connections with no traffic for timeout period (in seconds) are no longer reported
const defaultConnectionTimeout = 60

// Run the cleanup process every cleanupTime seconds
const cleanupTime = 120

// data to be exported to prometheus
// one data item per tuple
var (
	bytesSent = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "skydive_network_connection_total_bytes",
			Help: "Number of bytes that have been transmmitted on this connection",
		},
		[]string{"initiator_ip", "target_ip", "initiator_port", "target_port", "direction", "node_tid"},
	)
)

type storePrometheus struct {
	pipeline          *core.Pipeline // not currently used
	port              string
	connectionCache   *cache.Cache
	connectionTimeout int64
}

// we maintain a cache to keep track of connections that have been active/inactive
type cacheEntry struct {
	label1    prometheus.Labels
	label2    prometheus.Labels
	timeStamp int64
}

// SetPipeline setup; called by core/pipeline.NewPipeline
func (s *storePrometheus) SetPipeline(pipeline *core.Pipeline) {
	s.pipeline = pipeline
}

// StoreFlows store flows info in memory, before being shipped out
// For each flow reported, prepare prometheus entries, one for each direction of data flow.
func (s *storePrometheus) StoreFlows(flows map[core.Tag][]interface{}) error {
	secs := time.Now().Unix()
	for _, val := range flows {
		for _, i := range val {
			f := i.(*flow.Flow)
			if f.Transport == nil {
				continue
			}
			initiator_ip := f.Network.A
			target_ip := f.Network.B
			initiator_port := strconv.FormatInt(f.Transport.A, 10)
			target_port := strconv.FormatInt(f.Transport.B, 10)
			node_tid := f.NodeTID
			label1 := prometheus.Labels{
				"initiator_ip":   initiator_ip,
				"target_ip":      target_ip,
				"initiator_port": initiator_port,
				"target_port":    target_port,
				"direction":      "initiator_to_target",
				"node_tid":       node_tid,
			}
			label2 := prometheus.Labels{
				"initiator_ip":   initiator_ip,
				"target_ip":      target_ip,
				"initiator_port": initiator_port,
				"target_port":    target_port,
				"direction":      "target_to_initiator",
				"node_tid":       node_tid,
			}
			// post the info to prometheus
			bytesSent.With(label1).Set(float64(f.Metric.ABBytes))
			bytesSent.With(label2).Set(float64(f.Metric.BABytes))
			cEntry := cacheEntry{
				label1:    label1,
				label2:    label2,
				timeStamp: secs,
			}
			s.connectionCache.Set(f.L3TrackingID, cEntry, 0)
		}
	}
	return nil
}

// cleanupExpiredEntries - any entry that has expired should be removed from the prometheus reporting and cache
func (s *storePrometheus) cleanupExpiredEntries() {
	secs := time.Now().Unix()
	expireTime := secs - s.connectionTimeout
	entriesMap := s.connectionCache.Items()
	for k, v := range entriesMap {
		v2 := v.Object.(cacheEntry)
		if v2.timeStamp < expireTime {
			// clean up the entry
			logging.GetLogger().Debugf("secs = %s, deleting %s", secs, v2.label1)
			bytesSent.Delete(v2.label1)
			bytesSent.Delete(v2.label2)
			s.connectionCache.Delete(k)
		}
	}
}

func (s *storePrometheus) cleanupExpiredEntriesLoop() {
	for true {
		s.cleanupExpiredEntries()
		time.Sleep(cleanupTime * time.Second)
	}
}

// registerCollector - needed in order to send metrics to prometheus
func registerCollector(c prometheus.Collector) {
	prometheus.Register(c)
}

// startPrometheusInterface listens for prometheus resource usage requests
func startPrometheusInterface(s *storePrometheus) {

	// Metrics have to be registered to be exposed:
	registerCollector(bytesSent)

	// The Handler function provides a default handler to expose metrics
	// via an HTTP server. "/metrics" is the usual endpoint for that.
	http.Handle("/metrics", promhttp.Handler())

	err := http.ListenAndServe(s.port, nil)
	if err != nil {
		logging.GetLogger().Errorf("error on http.ListenAndServe = %s", err)
		os.Exit(1)
	}
}

// NewStorePrometheusInternal allocates and initializes the storePrometheus structure
func NewStorePrometheusInternal(cfg *viper.Viper) (*storePrometheus, error) {
	// process user defined parameters from yml file
	port_id := cfg.GetString(core.CfgRoot + "store.prom_sky_con.port")
	if port_id == "" {
		logging.GetLogger().Errorf("prometheus skydive port missing in configuration file")
		return nil, fmt.Errorf("Failed to detect port number")
	}
	logging.GetLogger().Infof("prometheus skydive port = %s", port_id)
	connectionTimeout := cfg.GetInt64(core.CfgRoot + "store.prom_sky_con.connection_timeout")
	if connectionTimeout == 0 {
		connectionTimeout = defaultConnectionTimeout
	}
	logging.GetLogger().Infof("connection timeout = %d", connectionTimeout)
	s := &storePrometheus{
		port:              ":" + port_id,
		connectionCache:   cache.New(0, 0),
		connectionTimeout: connectionTimeout,
	}
	return s, nil
}

// NewStorePrometheus returns a new interface for storing flows info to prometheus and starts the interface
func NewStorePrometheus(cfg *viper.Viper) (interface{}, error) {
	s, err := NewStorePrometheusInternal(cfg)
	if err != nil {
		return nil, err
	}
	go startPrometheusInterface(s)
	go s.cleanupExpiredEntriesLoop()
	return s, nil
}
