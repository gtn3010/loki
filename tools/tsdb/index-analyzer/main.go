package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/grafana/loki/v3/pkg/storage"
	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper"
	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper/tsdb"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	"github.com/grafana/loki/v3/tools/tsdb/helpers"
)

// go build ./tools/tsdb/index-analyzer && BUCKET=19453 DIR=/tmp/loki-index-analysis ./index-analyzer --config.file=/tmp/loki-config.yaml
func main() {
	conf, _, bucket, err := helpers.Setup()
	helpers.ExitErr("setting up", err)

	_, overrides, clientMetrics := helpers.DefaultConfigs()

	flag.Parse()

	periodCfg, tableRange, tableName, err := helpers.GetPeriodConfigForTableNumber(bucket, conf.SchemaConfig.Configs)
	helpers.ExitErr("find period config for bucket", err)

	objectClient, err := storage.NewObjectClient(periodCfg.ObjectType, conf.StorageConfig, clientMetrics)
	helpers.ExitErr("creating object client", err)

	shipper, err := indexshipper.NewIndexShipper(
		periodCfg.IndexTables.PathPrefix,
		conf.StorageConfig.TSDBShipperConfig,
		objectClient,
		overrides,
		nil,
		tsdb.OpenShippableTSDB,
		tableRange,
		prometheus.WrapRegistererWithPrefix("loki_tsdb_shipper_", prometheus.DefaultRegisterer),
		util_log.Logger,
	)
	helpers.ExitErr("creating index shipper", err)

	filteredTenants := os.Getenv("TENANTS")

	var tenants []string
	if len(filteredTenants) > 0 {
		tenants = strings.Split(filteredTenants, ",")
	} else {
		tenants, err = helpers.ResolveTenants(objectClient, periodCfg.IndexTables.PathPrefix, tableName)
		helpers.ExitErr("resolving tenants", err)
	}

	startTimeArg := os.Getenv("START")
	endTimeArg := os.Getenv("END")

	startTimeProm := model.Earliest
	if startTimeArg != "" {
		// Parse the input strings into time.Time
		startTime, err := time.Parse("2006-01-02 15:04:05", startTimeArg)
		if err != nil {
			fmt.Printf("Error parsing 'start' time: %v\n", err)
			os.Exit(1)
		}
		// Convert to Prometheus model.Time
		startTimeProm = model.Time(startTime.Unix())
	}

	endTimeProm := model.Latest
	if endTimeArg != "" {
		endTime, err := time.Parse("2006-01-02 15:04:05", endTimeArg)
		if err != nil {
		    fmt.Printf("Error parsing 'end' time: %v\n", err)
		    os.Exit(1)
		}
		endTimeProm = model.Time(endTime.Unix())
	}

	labels := os.Getenv("LABEL_FILTER")
	kvPairs := []string{}
	if labels != "" {
		kvPairs = strings.Split(labels, ",")
	}

	err = analyze(shipper, tableName, tenants, startTimeProm, endTimeProm, kvPairs)

	helpers.ExitErr("analyzing", err)
}
