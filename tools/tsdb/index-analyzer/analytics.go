package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper"
	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper/index"
	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper/tsdb"
	tsdb_index "github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper/tsdb/index"
)

func analyze(indexShipper indexshipper.IndexShipper, tableName string, tenants []string, from, to model.Time, kvPairs []string) error {

	var (
		series             int
		chunks             int
		seriesRes          []tsdb.Series
		chunkRes           []tsdb.ChunkRef
		maxChunksPerSeries int
		seriesOver1kChunks int
		labelMatchers      []*labels.Matcher
	)

	if len(kvPairs) > 0 {
		for _, kv := range kvPairs {
			s := strings.Split(kv, "=")
			labelMatchers = append(labelMatchers, labels.MustNewMatcher(labels.MatchEqual, s[0], s[1]))
		}
	} else {
		labelMatchers = append(labelMatchers, labels.MustNewMatcher(labels.MatchEqual, "", ""))
	}

	for _, tenant := range tenants {
		fmt.Printf("analyzing tenant %s from %s to %s with label matchers %v \n", tenant, from, to, labelMatchers)
		err := indexShipper.ForEach(
			context.Background(),
			tableName,
			tenant,
			index.ForEachIndexCallback(func(isMultiTenantIndex bool, idx index.Index) error {
				if isMultiTenantIndex {
					return nil
				}

				casted := idx.(*tsdb.TSDBFile)
				seriesRes = seriesRes[:0]
				chunkRes = chunkRes[:0]

				res, err := casted.Series(
					context.Background(),
					tenant,
					from,
					to,
					seriesRes, nil,
					labelMatchers...,
				)

				if err != nil {
					return err
				}

				fmt.Println("Labels with chunkRef: ", res)

				series += len(res)

				chunkRes, err := casted.GetChunkRefs(
					context.Background(),
					tenant,
					from,
					to,
					chunkRes, nil,
					labelMatchers...,
				)

				if err != nil {
					return err
				}

				for _, chunkRef := range chunkRes {
					fmt.Printf("ChunkRef %s %v from %s to %s \n", chunkRef.User, chunkRef.Fingerprint, chunkRef.Start, chunkRef.End)
				}

				chunks += len(chunkRes)

				err = casted.Index.(*tsdb.TSDBIndex).ForSeries(
					context.Background(),
					"", nil,
					model.Earliest,
					model.Latest,
					func(_ labels.Labels, _ model.Fingerprint, chks []tsdb_index.ChunkMeta) (stop bool) {
						if len(chks) > maxChunksPerSeries {
							maxChunksPerSeries = len(chks)
							if len(chks) > 1000 {
								seriesOver1kChunks++
							}
						}
						return false
					},
					labels.MustNewMatcher(labels.MatchEqual, "", ""),
				)

				if err != nil {
					return err
				}

				return nil
			}),
		)

		if err != nil {
			return err
		}
	}

	fmt.Printf("analyzed %d series and %d chunks for an average of %f chunks per series. max chunks/series was %d. number of series with over 1k chunks: %d\n", series, chunks, float64(chunks)/float64(series), maxChunksPerSeries, seriesOver1kChunks)

	return nil
}
