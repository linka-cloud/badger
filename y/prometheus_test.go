/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package y

import (
	"expvar"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestPrometheusCollectorExportsTypes(t *testing.T) {
	metrics := []struct {
		name       string
		metricType io_prometheus_client.MetricType
		labelKey   string
		labelVal   string
	}{
		{name: "badger_read_num_vlog", metricType: io_prometheus_client.MetricType_COUNTER},
		{name: "badger_read_bytes_vlog", metricType: io_prometheus_client.MetricType_COUNTER},
		{name: "badger_write_num_vlog", metricType: io_prometheus_client.MetricType_COUNTER},
		{name: "badger_write_bytes_vlog", metricType: io_prometheus_client.MetricType_COUNTER},
		{name: "badger_read_bytes_lsm", metricType: io_prometheus_client.MetricType_COUNTER},
		{name: "badger_write_bytes_l0", metricType: io_prometheus_client.MetricType_COUNTER},
		{name: "badger_write_bytes_compaction", metricType: io_prometheus_client.MetricType_COUNTER, labelKey: "key", labelVal: "prom_test"},
		{name: "badger_get_num_lsm", metricType: io_prometheus_client.MetricType_COUNTER, labelKey: "key", labelVal: "prom_test"},
		{name: "badger_hit_num_lsm_bloom_filter", metricType: io_prometheus_client.MetricType_COUNTER, labelKey: "key", labelVal: "prom_test"},
		{name: "badger_get_num_memtable", metricType: io_prometheus_client.MetricType_COUNTER},
		{name: "badger_get_num_user", metricType: io_prometheus_client.MetricType_COUNTER},
		{name: "badger_put_num_user", metricType: io_prometheus_client.MetricType_COUNTER},
		{name: "badger_write_bytes_user", metricType: io_prometheus_client.MetricType_COUNTER},
		{name: "badger_get_with_result_num_user", metricType: io_prometheus_client.MetricType_COUNTER},
		{name: "badger_iterator_num_user", metricType: io_prometheus_client.MetricType_COUNTER},
		{name: "badger_size_bytes_lsm", metricType: io_prometheus_client.MetricType_GAUGE, labelKey: "key", labelVal: "prom_test"},
		{name: "badger_size_bytes_vlog", metricType: io_prometheus_client.MetricType_GAUGE, labelKey: "key", labelVal: "prom_test"},
		{name: "badger_write_pending_num_memtable", metricType: io_prometheus_client.MetricType_GAUGE, labelKey: "key", labelVal: "prom_test"},
		{name: "badger_compaction_current_num_lsm", metricType: io_prometheus_client.MetricType_GAUGE},
	}

	restores := []func(){
		setIntMetric(numReadsVlog, 1),
		setIntMetric(numBytesReadVlog, 2),
		setIntMetric(numWritesVlog, 3),
		setIntMetric(numBytesVlogWritten, 4),
		setIntMetric(numBytesReadLSM, 5),
		setIntMetric(numBytesWrittenToL0, 6),
		setIntMetric(numMemtableGets, 7),
		setIntMetric(numGets, 8),
		setIntMetric(numPuts, 9),
		setIntMetric(numBytesWrittenUser, 10),
		setIntMetric(numGetsWithResults, 11),
		setIntMetric(numIteratorsCreated, 12),
		setIntMetric(numCompactionTables, 1),
		setMapMetric(numBytesCompactionWritten, "prom_test", 13),
		setMapMetric(numLSMGets, "prom_test", 14),
		setMapMetric(numLSMBloomHits, "prom_test", 15),
		setMapMetric(lsmSize, "prom_test", 16),
		setMapMetric(vlogSize, "prom_test", 17),
		setMapMetric(pendingWrites, "prom_test", 18),
	}

	defer func() {
		for _, restore := range restores {
			restore()
		}
	}()

	registry := prometheus.NewRegistry()
	collector := NewPrometheusCollector()
	registry.MustRegister(collector)

	mfs, err := registry.Gather()
	require.NoError(t, err)

	mfByName := make(map[string]*io_prometheus_client.MetricFamily, len(mfs))
	for _, mf := range mfs {
		mfByName[mf.GetName()] = mf
	}

	for _, entry := range metrics {
		t.Run(entry.name, func(t *testing.T) {
			mf := mfByName[entry.name]
			require.NotNil(t, mf)
			require.Equal(t, entry.metricType, mf.GetType())
			require.NotEmpty(t, mf.Metric)

			if entry.labelKey != "" {
				metric := findMetricWithLabel(mf, entry.labelKey, entry.labelVal)
				require.NotNil(t, metric)
				require.Len(t, metric.Label, 1)
				require.Equal(t, entry.labelKey, metric.Label[0].GetName())
				require.Equal(t, entry.labelVal, metric.Label[0].GetValue())
				return
			}

			require.Len(t, mf.Metric[0].Label, 0)
		})
	}
}

func setIntMetric(metric *expvar.Int, val int64) func() {
	prev := metric.Value()
	metric.Set(val)
	return func() {
		metric.Set(prev)
	}
}

func setMapMetric(metric *expvar.Map, key string, val int64) func() {
	var prev int64
	var hadPrev bool
	if existing := metric.Get(key); existing != nil {
		if existingInt, ok := existing.(*expvar.Int); ok {
			prev = existingInt.Value()
			hadPrev = true
		}
	}

	newVal := &expvar.Int{}
	newVal.Set(val)
	metric.Set(key, newVal)

	return func() {
		if !hadPrev {
			resetVal := &expvar.Int{}
			resetVal.Set(0)
			metric.Set(key, resetVal)
			return
		}
		resetVal := &expvar.Int{}
		resetVal.Set(prev)
		metric.Set(key, resetVal)
	}
}

func findMetricWithLabel(mf *io_prometheus_client.MetricFamily, labelName string, labelValue string) *io_prometheus_client.Metric {
	for _, metric := range mf.Metric {
		for _, label := range metric.Label {
			if label.GetName() == labelName && label.GetValue() == labelValue {
				return metric
			}
		}
	}
	return nil
}
