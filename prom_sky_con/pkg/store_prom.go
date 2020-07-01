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
	"strconv"
	"net/http"

	"github.com/spf13/viper"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/skydive-project/skydive/flow"
	"github.com/skydive-project/skydive/logging"
	"github.com/skydive-project/skydive-flow-exporter/core"
)

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
	pipeline  *core.Pipeline
	port string
}

// SetPipeline setup
func (s *storePrometheus) SetPipeline(pipeline *core.Pipeline) {
	s.pipeline = pipeline
}

// StoreFlows store flows in memory, before being written to the object store
func (s *storePrometheus) StoreFlows(flows map[core.Tag][]interface{}) error {
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
			label := prometheus.Labels {
				"initiator_ip": initiator_ip,
				"target_ip": target_ip,
				"initiator_port": initiator_port,
				"target_port": target_port,
				"direction": direction,
				"node_tid": node_tid,
				}
			bytesSent.With(label).Set(float64(f.Metric.ABBytes))
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
		}
	}
	return nil
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

	//http.ListenAndServe(":9080", nil)
	http.ListenAndServe(s.port, nil)
}

// NewStorePrometheus returns a new interface for storing flows info to prometheus
func NewStorePrometheus(cfg *viper.Viper) (interface{}, error) {
	port_id := cfg.GetString(core.CfgRoot + "store.prom_sky_con.port")
	if  port_id == "" {
		logging.GetLogger().Errorf("prometheus skydive port missing in configuration file")
		return nil, fmt.Errorf("Failed to detect port number")
	}
	logging.GetLogger().Infof("prometheus skydive port = %s", port_id)
	s := &storePrometheus{
		port: ":" + port_id,
	}
	go startPrometheusInterface(s)
	return s, nil
}
