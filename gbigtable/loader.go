package gbigtable

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/google/uuid"
	"github.com/teltech/logger"
	"github.com/zpiroux/geist/entity"
)

const (
	PreDefinedRowKeyTimestampIso      = "timestampIso"
	PreDefinedRowKeyUuid              = "uuid"
	PreDefinedRowKeyInvertedTimestamp = "invertedTimestamp"
	PreDefinedRowKeyKeysInMap         = "keysInMap"

	GarbageCollectionPolicyMaxVersions = "maxVersions"
	GarbageCollectionPolicyMaxAge      = "maxAge"
)

const (
	isoTimestampLayoutMilliseconds = "2006-01-02T15:04:05.000Z"
	openTableRetryCount            = 5               // not important to be added to config for now
	openTableSleepPeriod           = 2 * time.Second // not important to be added to config for now
)

type RowKeyValue struct {
	RowKey string
	Value  []byte
}

var log *logger.Log

func init() {
	log = logger.New()
}

type loader struct {
	id           string
	spec         *entity.Spec
	sinkConfig   SinkConfig
	client       BigTableClient
	adminClient  BigTableAdminClient
	openedTables map[string]BigTableTable
}

func newLoader(
	ctx context.Context,
	spec *entity.Spec,
	id string,
	client BigTableClient,
	adminClient BigTableAdminClient) (*loader, error) {

	if isNil(client) || isNil(adminClient) {
		return nil, errors.New("invalid arguments, clients cannot be nil")
	}
	sinkConfig, err := NewSinkConfig(spec)
	if err != nil {
		return nil, err
	}
	var l = loader{
		id:          id,
		spec:        spec,
		sinkConfig:  sinkConfig,
		client:      client,
		adminClient: adminClient,
	}

	l.openedTables = make(map[string]BigTableTable)
	err = l.createTables(ctx)
	if err != nil {

		if otherStreamCreatingTable(err) {
			log.Warnf(l.lgprfx()+"another executor's bigtable sink just created the table %+v, just opening it instead", sinkConfig.Tables)
		} else {
			return &l, err
		}
	}

	for i := 0; i < openTableRetryCount; i++ {

		err = l.openTables(ctx)

		if err == nil {
			break
		}

		if otherStreamCreatingTable(err) {
			time.Sleep(openTableSleepPeriod)
		}
	}

	return &l, err
}

// No good granular way to properly get real error codes from bt client, to detect these "non-errors".
// Need to parse error string -.-
func otherStreamCreatingTable(err error) bool {
	return strings.Contains(err.Error(), "AlreadyExists") ||
		strings.Contains(err.Error(), "Table currently being created") ||
		strings.Contains(err.Error(), "is creating")
}

// The GEIST BigTable loader implementation only supports a single input Transformed object,
// since using the EventSplit transform doesn't make sense in this case.
func (l *loader) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {
	var (
		err    error
		rowKey string
	)
	retryable := true
	if data == nil {
		return rowKey, errors.New("StreamLoad called without data to load"), false
	}

	for _, table := range l.sinkConfig.Tables {
		if l.applicableEvent(ctx, table, data[0]) {

			if table.RowKey.Predefined == PreDefinedRowKeyKeysInMap {
				rowKey, err, retryable = l.upsertDataFromMap(ctx, table, data[0])
			} else {
				rowKey, err, retryable = l.upsertData(ctx, table, data[0])
			}

			if err != nil {
				return rowKey, err, retryable
			}

		} else {
			log.Debug(l.lgprfx() + "not applicable event")
			retryable = false
		}
	}

	return rowKey, err, retryable
}

func (l *loader) Shutdown(ctx context.Context) {
	// Nothing to shut down
}

func (l *loader) upsertDataFromMap(ctx context.Context, table Table, event *entity.Transformed) (string, error, bool) {

	var (
		err    error
		rowKey string
	)
	retryable := false
	singleRowTransform := entity.NewTransformed()

	rowItems, ok := event.Data[table.RowKey.MapId]
	if !ok {
		err = fmt.Errorf(l.lgprfx()+"sink spec specified inserting rows from map items but no map found in transformed input, table spec: %+v, event: %s", table, event)
		return "", err, false
	}

	rowItemsMap, ok := rowItems.(map[string]any)
	if !ok {
		err = fmt.Errorf(l.lgprfx()+"bug - upsertDataFromMap() item map type to be map[string]any, table spec: %+v, event: %s", table, event)
		return "", err, false
	}

	// Create new virtual input transformed to be inserted as rows, to re-use standard upsert flow
	for key, item := range rowItemsMap {

		var rkv RowKeyValue
		switch item := item.(type) {
		case string:
			rkv.Value = []byte(item)
		case []byte:
			rkv.Value = item
		default:
			err = fmt.Errorf(l.lgprfx()+"invalid stream spec, inserting from map requires string or byte item types, table spec: %+v, event: %s", table, event)
			return "", err, false
		}
		rkv.RowKey = key
		singleRowTransform.Data[table.RowKey.MapId] = rkv
		rowKey, err, retryable = l.upsertData(ctx, table, singleRowTransform)
		if err != nil {
			return rowKey, err, retryable
		}
	}

	return rowKey, err, retryable
}

func (l *loader) upsertData(ctx context.Context, table Table, event *entity.Transformed) (string, error, bool) {

	if event == nil {
		return "", errors.New("upsertData received nil event"), false
	}

	mut, err, retryable := l.createMutation(ctx, table, event)
	if err != nil {
		return "", err, retryable
	}

	rowKey := l.createRowKey(ctx, table, event)
	if rowKey == "" {
		return rowKey, errors.New("created rowKey is empty"), false
	}

	t := l.openedTables[table.Name]
	if t == nil {
		return rowKey, fmt.Errorf("could not find opened table %s, when inserting row with rowKey: %s, mut: %v", table.Name, rowKey, mut), false
	}
	if err := t.Apply(ctx, rowKey, mut); err != nil {
		return rowKey, fmt.Errorf("table.Apply() failed with: %v, rowKey: %s, mut: %v", err, rowKey, mut), true
	}

	if l.spec.Ops.LogEventData {
		log.Infof(l.lgprfx()+"(table: %s) Successfully wrote row with key: %s", table.Name, rowKey)
	}
	return rowKey, nil, true
}

func (l *loader) createMutation(ctx context.Context, table Table, event *entity.Transformed) (*bigtable.Mutation, error, bool) {
	var value []byte
	mut := bigtable.NewMutation()
	timestamp := bigtable.Now()

	for _, family := range table.ColumnFamilies {
		for _, column := range family.ColumnQualifiers {

			// For specs with multiple event types into a single table, ignore irrelevant events
			if _, ok := event.Data[column.Id]; !ok {
				continue
			}

			switch event.Data[column.Id].(type) {
			case string:
				value = []byte(event.Data[column.Id].(string))
			case int:
				value = []byte(strconv.Itoa(event.Data[column.Id].(int)))
			case []byte:
				value = event.Data[column.Id].([]byte)
			case RowKeyValue:
				value = event.Data[column.Id].(RowKeyValue).Value
			default:
				return nil, fmt.Errorf("unsupported type in Transformed: %#v, when upserting to BT", event.Data[column.Id]), false
			}
			columnName, err := l.generateColumnName(column, event)

			if err != nil {
				return nil, err, false
			}

			mut.Set(family.Name, columnName, timestamp, value)
		}
	}
	return mut, nil, false

}

func (l *loader) generateColumnName(column ColumnQualifier, event *entity.Transformed) (string, error) {

	var suffix string

	if len(column.Name) > 0 {
		return column.Name, nil
	}

	suffixValue := event.Data[column.NameFromId.SuffixFromId]

	switch suffixValue := suffixValue.(type) {
	case string:
	case int:
		suffix = strconv.Itoa(suffixValue)
	case int64:
		suffix = strconv.Itoa(int(suffixValue))
	default:
		return "", fmt.Errorf("invalid suffix type: %T, could not create column name for column: %#v, event: %s", suffixValue, column, event.String())
	}

	return column.NameFromId.Prefix + suffix, nil
}

func (l *loader) createRowKey(ctx context.Context, table Table, event *entity.Transformed) string {
	var (
		rowKey string
		key    string
	)

	switch table.RowKey.Predefined {

	case PreDefinedRowKeyTimestampIso:
		return time.Now().Format(isoTimestampLayoutMilliseconds)
	case PreDefinedRowKeyUuid:
		return uuid.New().String()
	case PreDefinedRowKeyInvertedTimestamp:
		return strconv.FormatInt(invertedTimestamp(), 10)
	case PreDefinedRowKeyKeysInMap:
		rowKeyValue, ok := event.Data[table.RowKey.MapId]
		if !ok {
			log.Errorf(l.lgprfx()+"invalid stream spec, can't find row key in PreDefinedRowKeyKeysInMap mode, table spec: %+v, event data: %s", table, event)
			return ""
		}
		rkv, ok := rowKeyValue.(RowKeyValue)
		if ok {
			return rkv.RowKey
		}
		return ""
	}

	for i, keyId := range table.RowKey.Keys {
		switch event.Data[keyId].(type) {
		case time.Time:
			key = event.Data[keyId].(time.Time).Format(isoTimestampLayoutMilliseconds)
		default:
			key = fmt.Sprintf("%v", event.Data[keyId])
		}
		if i > 0 {
			rowKey = rowKey + table.RowKey.Delimiter
		}
		rowKey = rowKey + key
	}
	return rowKey
}

func invertedTimestamp() int64 {
	return math.MaxInt64 - time.Now().UnixNano()
}

// Returns true if the specified table has a whitelisting matching incoming event.
// It also returns true if there's no Whitelisting config in the spec, allowing all events.
// This is mostly used in multi-table Sink specs, since otherwise full event filtering is handled in
// the Transform part of the spec.
func (l *loader) applicableEvent(ctx context.Context, table Table, event *entity.Transformed) bool {

	if table.Whitelist == nil {
		return true
	}
	if event == nil {
		log.Errorf("applicableEvent received nil event for table %s", table.Name)
		return false
	}
	if len(table.Whitelist.Id) == 0 {
		return true
	}
	eventKeyValue := event.Data[table.Whitelist.Id]
	return sliceContains(table.Whitelist.Values, eventKeyValue.(string))
}

func (l *loader) createTables(ctx context.Context) error {

	tables, err := l.adminClient.Tables(ctx)
	if err != nil {
		return fmt.Errorf("could not fetch table list: %v", err)
	}

	for _, table := range l.sinkConfig.Tables {

		if !sliceContains(tables, table.Name) {
			if err := l.adminClient.CreateTable(ctx, table.Name); err != nil {
				return fmt.Errorf("could not create table %s: %v", table.Name, err)
			}
		}

		tblInfo, err := l.adminClient.TableInfo(ctx, table.Name)
		if err != nil {
			return fmt.Errorf("could not read info for table %s: %v", table.Name, err)
		}

		if err = l.createColumnFamilies(ctx, table, tblInfo); err != nil {
			return err
		}
	}

	return err
}

func (l *loader) createColumnFamilies(ctx context.Context, table Table, tblInfo *bigtable.TableInfo) error {

	for _, columnFamily := range table.ColumnFamilies {
		if !sliceContains(tblInfo.Families, columnFamily.Name) {
			if err := l.adminClient.CreateColumnFamily(ctx, table.Name, columnFamily.Name); err != nil {
				return fmt.Errorf("could not create column family %s: %v", columnFamily.Name, err)
			}

			if err := l.setGCPolicy(ctx, table.Name, columnFamily); err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *loader) setGCPolicy(ctx context.Context, tableName string, columnFamily ColumnFamily) error {

	switch columnFamily.GarbageCollectionPolicy.Type {

	case GarbageCollectionPolicyMaxVersions:
		policy := bigtable.MaxVersionsPolicy(columnFamily.GarbageCollectionPolicy.Value)
		if err := l.adminClient.SetGCPolicy(ctx, tableName, columnFamily.Name, policy); err != nil {
			return fmt.Errorf("SetGCPolicy(%s): %v", policy, err)
		}

	case GarbageCollectionPolicyMaxAge:
		maxAgeHours := time.Hour * time.Duration(columnFamily.GarbageCollectionPolicy.Value)
		policy := bigtable.MaxAgePolicy(maxAgeHours)
		log.Debugf(l.lgprfx()+"setting maxAge/ttl to %+v", maxAgeHours)
		if err := l.adminClient.SetGCPolicy(ctx, tableName, columnFamily.Name, policy); err != nil {
			return fmt.Errorf("SetGCPolicy(%s): %v", policy, err)
		}
	}

	return nil
}

func sliceContains(list []string, target string) bool {
	for _, s := range list {
		if s == target {
			return true
		}
	}
	return false
}

func (l *loader) openTables(ctx context.Context) error {

	for _, table := range l.sinkConfig.Tables {

		t := l.client.Open(table.Name)
		if t == nil {
			return fmt.Errorf("could not open table %s", table.Name)
		}
		l.openedTables[table.Name] = t
	}
	return nil
}

func (g *loader) setOpenTables(b map[string]BigTableTable) {
	g.openedTables = b
}

func (l *loader) lgprfx() string {
	return "[xbigtable.loader:" + l.id + "] "
}
