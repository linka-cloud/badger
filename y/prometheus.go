/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"expvar"
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// PrometheusCollector exposes Badger expvar metrics to Prometheus.
type PrometheusCollector struct {
	mu          sync.RWMutex
	descs       map[prometheusDescKey]*prometheus.Desc
	metricTypes map[expvar.Var]prometheus.ValueType
}

type prometheusDescKey struct {
	name     string
	hasLabel bool
}

// NewPrometheusCollector returns a Prometheus collector that exports expvar metrics.
func NewPrometheusCollector() *PrometheusCollector {
	return &PrometheusCollector{
		descs: make(map[prometheusDescKey]*prometheus.Desc),
		metricTypes: map[expvar.Var]prometheus.ValueType{
			lsmSize:             prometheus.GaugeValue,
			vlogSize:            prometheus.GaugeValue,
			pendingWrites:       prometheus.GaugeValue,
			numCompactionTables: prometheus.GaugeValue,
		},
	}
}

// Describe sends the descriptors of each metric to the provided channel.
func (c *PrometheusCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.collectDescs() {
		ch <- desc
	}
}

// Collect sends the metrics to the provided channel.
func (c *PrometheusCollector) Collect(ch chan<- prometheus.Metric) {
	expvar.Do(func(kv expvar.KeyValue) {
		c.collectVar(ch, kv.Key, kv.Value)
	})
}

func (c *PrometheusCollector) collectDescs() []*prometheus.Desc {
	var descs []*prometheus.Desc
	expvar.Do(func(kv expvar.KeyValue) {
		switch kv.Value.(type) {
		case *expvar.Int, *expvar.Float:
			descs = append(descs, c.descFor(kv.Key, false))
		case *expvar.Map:
			descs = append(descs, c.descFor(kv.Key, true))
		}
	})

	return descs
}

func (c *PrometheusCollector) collectVar(ch chan<- prometheus.Metric, name string, v expvar.Var) {
	switch t := v.(type) {
	case *expvar.Int:
		c.emitMetric(ch, name, float64(t.Value()), nil, c.metricTypeFor(t))
	case *expvar.Float:
		c.emitMetric(ch, name, t.Value(), nil, c.metricTypeFor(t))
	case *expvar.Map:
		metricType := c.metricTypeFor(t)
		t.Do(func(kv expvar.KeyValue) {
			val, ok := valueFromVar(kv.Value)
			if !ok {
				return
			}
			c.emitMetric(ch, name, val, []string{kv.Key}, metricType)
		})
	}
}

func (c *PrometheusCollector) emitMetric(ch chan<- prometheus.Metric, name string, val float64, labels []string, metricType prometheus.ValueType) {
	desc := c.descFor(name, len(labels) > 0)
	metric, err := prometheus.NewConstMetric(desc, metricType, val, labels...)
	if err != nil {
		return
	}
	ch <- metric
}

func (c *PrometheusCollector) descFor(name string, hasLabel bool) *prometheus.Desc {
	key := prometheusDescKey{name: name, hasLabel: hasLabel}

	c.mu.RLock()
	if desc, ok := c.descs[key]; ok {
		c.mu.RUnlock()
		return desc
	}
	c.mu.RUnlock()

	metricName := name

	var labelNames []string
	if hasLabel {
		labelNames = []string{"key"}
	}

	desc := prometheus.NewDesc(metricName, "Badger expvar metric "+name, labelNames, nil)

	c.mu.Lock()
	c.descs[key] = desc
	c.mu.Unlock()

	return desc
}

func valueFromVar(v expvar.Var) (float64, bool) {
	switch t := v.(type) {
	case *expvar.Int:
		return float64(t.Value()), true
	case *expvar.Float:
		return t.Value(), true
	case *expvar.String:
		parsed, err := strconv.ParseFloat(t.Value(), 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func (c *PrometheusCollector) metricTypeFor(v expvar.Var) prometheus.ValueType {
	if metricType, ok := c.metricTypes[v]; ok {
		return metricType
	}
	return prometheus.CounterValue
}
