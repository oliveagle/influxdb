package migration

import (
	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/protocol"
)

type Coordinator interface {
	WriteSeriesData(common.User, string, []*protocol.Series) error
}
