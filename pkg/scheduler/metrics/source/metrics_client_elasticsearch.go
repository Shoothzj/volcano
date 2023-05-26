/*
 Copyright 2023 The Volcano Authors.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package source

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v7"
	"net/http"
	"strings"
	"time"
)

const (
	// esHostNameField is the field name of host name in the document
	esHostNameField = "host.hostname"
	// esCpuUsageField is the field name of cpu usage in the document
	esCpuUsageField = "host.cpu.usage"
	// esMemUsageField is the field name of mem usage in the document
	esMemUsageField = "system.memory.actual.used.pct"
)

type ElasticsearchMetricsClient struct {
	address           string
	indexName         string
	es                *elasticsearch.Client
	hostnameFieldName string
}

func NewElasticsearchMetricsClient(address string, conf map[string]string) (*ElasticsearchMetricsClient, error) {
	e := &ElasticsearchMetricsClient{address: address}
	indexName := conf["elasticsearch.index"]
	if len(indexName) == 0 {
		e.indexName = "metricbeat-*"
	} else {
		e.indexName = indexName
	}
	hostNameFieldName := conf["elasticsearch.hostnameFieldName"]
	if len(hostNameFieldName) == 0 {
		e.hostnameFieldName = esHostNameField
	} else {
		e.hostnameFieldName = hostNameFieldName
	}
	var err error
	insecureSkipVerify := conf["tls.insecureSkipVerify"] == "true"
	e.es, err = elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{address},
		Username:  conf["elasticsearch.username"],
		Password:  conf["elasticsearch.password"],
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: insecureSkipVerify,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return e, nil
}

func (e *ElasticsearchMetricsClient) NodeMetricsAvg(ctx context.Context, period string, nodes ...string) (map[string]NodeMetrics, error) {
	nodeMetrics := make(map[string]NodeMetrics)
	var buf bytes.Buffer
	query := map[string]interface{}{
		"size": 0,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"range": map[string]interface{}{
							"@timestamp": map[string]interface{}{
								"gte": "now-" + period,
								"lt":  "now",
							},
						},
					},
					{
						"terms": map[string]interface{}{
							e.hostnameFieldName: nodes,
						},
					},
				},
			},
		},
		"aggs": map[string]interface{}{
			"node": map[string]interface{}{
				"terms": map[string]interface{}{
					"field": e.hostnameFieldName,
				},
				"aggs": map[string]interface{}{
					"cpu": map[string]interface{}{
						"avg": map[string]interface{}{
							"field": esCpuUsageField,
						},
					},
					"mem": map[string]interface{}{
						"avg": map[string]interface{}{
							"field": esMemUsageField,
						},
					},
				},
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return nil, err
	}
	res, err := e.es.Search(
		e.es.Search.WithContext(ctx),
		e.es.Search.WithIndex(e.GetIndex()...),
		e.es.Search.WithBody(&buf),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	var r struct {
		Aggregations struct {
			Node struct {
				Buckets []struct {
					Key string `json:"key"`
					Cpu struct {
						Value float64 `json:"value"`
					}
					Mem struct {
						Value float64 `json:"value"`
					}
				}
			}
		} `json:"aggregations"`
	}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return nil, err
	}

	for _, node := range r.Aggregations.Node.Buckets {
		// The data obtained from Elasticsearch is in decimals and needs to be multiplied by 100.
		nodeMetrics[node.Key] = NodeMetrics{
			Cpu:    node.Cpu.Value * 100,
			Memory: node.Mem.Value * 100,
		}
	}
	return nodeMetrics, nil
}

func (e *ElasticsearchMetricsClient) GetIndex() []string {
	var indexs []string
	if strings.Contains(e.indexName, "{{DATE}}") {
		timestamp := time.Now()
		indexs = append(indexs, strings.ReplaceAll(e.indexName, "{{DATE}}", timestamp.Format("2006.01.02")))
	} else if strings.Contains(e.indexName, "{{TIME}}") {
		// Compatible with cross hour scenarios
		timestamp := time.Now()
		indexs = append(indexs, strings.ReplaceAll(e.indexName, "{{TIME}}", timestamp.Add(-time.Hour).Format("2006.01.02-15")))
		indexs = append(indexs, strings.ReplaceAll(e.indexName, "{{TIME}}", timestamp.Format("2006.01.02-15")))
	} else {
		indexs = append(indexs, e.indexName)
	}
	return indexs
}
