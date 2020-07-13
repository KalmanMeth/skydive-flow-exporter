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

package core

import (
	"fmt"
	"os"
	"strconv"
	"time"
	"net/http"

	cache "github.com/pmylund/go-cache"
	"github.com/spf13/viper"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/skydive-project/skydive/flow"
	"github.com/skydive-project/skydive/logging"
	"github.com/skydive-project/skydive-flow-exporter/core"
)

const defaultConnectionTimeout = 5

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
	// pipeline  *core.Pipeline // not currently used
	port string
	connectionCache *cache.Cache
	connectionTimeout int64
}

type cacheEntry struct {
	label1 prometheus.Labels
	label2 prometheus.Labels
	timeStamp int64
}

// SetPipeline setup; called by core/pipeline.NewPipeline
func (s *storePrometheus) SetPipeline(pipeline *core.Pipeline) {
	// s.pipeline = pipeline
}

// StoreFlows store flows in memory, before being written to the object store
func (s *storePrometheus) StoreFlows(flows map[core.Tag][]interface{}) error {
	secs := time.Now().Unix()
	for _, val := range flows {
		for _, i := range val {
			f := i.(*flow.Flow)
			if f.Transport == nil {
				continue
			}
			logging.GetLogger().Debugf("flow = %s", f)
			initiator_ip := f.Network.A
			target_ip := f.Network.B
			initiator_port := strconv.FormatInt(f.Transport.A, 10)
			target_port := strconv.FormatInt(f.Transport.B, 10)
			node_tid := f.NodeTID
			direction := "initiator_to_target"
			label1 := prometheus.Labels {
				"initiator_ip": initiator_ip,
				"target_ip": target_ip,
				"initiator_port": initiator_port,
				"target_port": target_port,
				"direction": direction,
				"node_tid": node_tid,
				}
			bytesSent.With(label1).Set(float64(f.Metric.ABBytes))
			direction = "target_to_initiator"
			label2 := prometheus.Labels {
				"initiator_ip": initiator_ip,
				"target_ip": target_ip,
				"initiator_port": initiator_port,
				"target_port": target_port,
				"direction": direction,
				"node_tid": node_tid,
				}
			bytesSent.With(label2).Set(float64(f.Metric.BABytes))
			cEntry := cacheEntry {
				label1: label1,
				label2: label2,
				timeStamp: secs,
			}
			s.connectionCache.Set(f.L3TrackingID, cEntry, 0)
		}
	}
	s.cleanupExpiredEntries()
	return nil
}

// cleanupExpiredEntries - any entry that has expired should be removed from the prometheus reporting and cache
func (s *storePrometheus) cleanupExpiredEntries() {
	secs := time.Now().Unix()
	expireTime := secs - s.connectionTimeout
	entriesMap := s.connectionCache.Items()
	for  k, v := range entriesMap {
		v2 := v.Object.(cacheEntry)
		if v2.timeStamp < expireTime {
			// clean up the entry
			bytesSent.Delete(v2.label1)
			bytesSent.Delete(v2.label2)
			s.connectionCache.Delete(k)
		}
	}
}

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

// NewStorePrometheus returns a new interface for storing flows info to prometheus and starts the interface
func NewStorePrometheus(cfg *viper.Viper) (interface{}, error) {
	port_id := cfg.GetString(core.CfgRoot + "store.prom_sky_con.port")
	if  port_id == "" {
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
		port: ":" + port_id,
		connectionCache: cache.New(0, 0),
		connectionTimeout: connectionTimeout,
	}
	go startPrometheusInterface(s)
	return s, nil
}
