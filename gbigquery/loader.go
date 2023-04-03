package gbigquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/teltech/logger"
	"github.com/zpiroux/geist/entity"
	"github.com/zpiroux/geist/pkg/notify"
)

const (
	TableUpdateBackoffTime         = 8 * time.Second
	DefaultBigQueryDatasetLocation = "EU"
)

type loader struct {
	id       string
	spec     *entity.Spec
	table    *bigquery.Table
	metadata *bigquery.TableMetadata
	client   BigQueryClient
	inserter BigQueryInserter
	mdMutex  *sync.Mutex
	notifier *notify.Notifier
}

func newLoader(
	ctx context.Context,
	c entity.Config,
	client BigQueryClient,
	metadataMutex *sync.Mutex) (*loader, error) {

	if isNil(client) {
		return nil, errors.New("invalid arguments, BigQueryClient cannot be nil")
	}
	l := loader{
		id:      c.ID,
		spec:    c.Spec,
		client:  client,
		mdMutex: metadataMutex,
	}
	var log *logger.Log
	if c.Log {
		log = logger.New()
	}
	l.notifier = notify.New(c.NotifyChan, log, 2, "gbigquery.loader", c.ID, c.Spec.Id())
	err := l.init(ctx)
	return &l, err
}

func (l *loader) StreamLoad(ctx context.Context, data []*entity.Transformed) (string, error, bool) {

	rows, newColumns, err := l.createRows(data)
	if err != nil {
		return "", err, false // This is probably an unretryable error due to corrupt schema or event
	}

	if len(rows) == 0 {
		l.notifier.Notify(entity.NotifyLevelWarn, "No rows to be inserted from transformed data, might be an error in stream spec")
		return "", nil, false
	}

	if err := l.handleDynamicColumnUpdates(ctx, newColumns); err != nil {
		return "", err, true
	}

	err = l.insertRows(ctx, rows)

	if err != nil && probableTableUpdatingError(err) {
		// If new column(s) been added by table update it can take some time before BQ allows inserts to the new
		// column. Until then insert will return with insert failed errors. To reduce request retries in this case
		// back off slightly.
		l.notifier.Notify(entity.NotifyLevelWarn, "BQ table probably not ready after table update, let's back off a few sec (err: %v)", err)
		if !sleepCtx(ctx, TableUpdateBackoffTime) {
			err = entity.ErrEntityShutdownRequested
		}
	}

	if err == nil && l.spec.Ops.LogEventData {
		l.notifier.Notify(entity.NotifyLevelDebug, "Successfully inserted %d rows to BigQuery table %s", len(rows), l.table.TableID)
	}

	return "", err, true
}

func probableTableUpdatingError(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "no such field")
}

func (l *loader) Shutdown(ctx context.Context) {
	// Nothing to shut down
}

func (l *loader) init(ctx context.Context) error {

	var (
		err    error
		status DatasetTableStatus
	)

	if len(l.spec.Sink.Config.Tables) == 0 {
		return fmt.Errorf("no BigQuery table specified in spec: %v", l.spec)
	}
	tableSpec := l.spec.Sink.Config.Tables[0]
	l.table = l.client.CreateTableRef(tableSpec.Dataset, tableSpec.Name)

	// TODO: Add back-off-retry here, if init called from NewLoader()

	l.mdMutex.Lock()
	defer l.mdMutex.Unlock()

	_, status, err = l.client.GetDatasetMetadata(ctx, l.client.CreateDatasetRef(tableSpec.Dataset))
	if err != nil && status == Unknown {
		return err
	}

	if err = l.ensureDatasetPresent(ctx, tableSpec, status); err != nil {
		return err
	}

	l.metadata, status, err = l.client.GetTableMetadata(ctx, l.client.CreateTableRef(tableSpec.Dataset, tableSpec.Name))
	if err != nil && status == Unknown {
		return err
	}

	if err = l.ensureTablePresent(ctx, tableSpec, status); err != nil {
		return err
	}

	l.inserter = l.client.GetTableInserter(l.table)
	return err
}

func (l *loader) ensureDatasetPresent(ctx context.Context, tableSpec entity.Table, status DatasetTableStatus) error {
	if status != NonExistent {
		return nil
	}

	md := &bigquery.DatasetMetadata{
		Location: DefaultBigQueryDatasetLocation,
	}
	if tableSpec.DatasetCreation != nil {
		md.Description = tableSpec.DatasetCreation.Description
		if tableSpec.DatasetCreation.Location != "" {
			md.Location = tableSpec.DatasetCreation.Location
		}
	}
	if err := l.client.CreateDataset(ctx, tableSpec.Dataset, md); err != nil {
		return err
	}

	return nil
}

func (l *loader) ensureTablePresent(ctx context.Context, tableSpec entity.Table, status DatasetTableStatus) (err error) {
	if status != NonExistent {
		return nil
	}
	l.metadata, err = l.createTableMetadata(ctx, tableSpec)
	if err != nil {
		return err
	}
	l.table, err = l.client.CreateTable(ctx, tableSpec.Dataset, tableSpec.Name, l.metadata)
	return err
}

// Creates table meta data based on config in stream spec, for use in table creation
func (l *loader) createTableMetadata(ctx context.Context, tableSpec entity.Table) (*bigquery.TableMetadata, error) {

	columns, err := l.createTableMetadataColumns(tableSpec)
	if err != nil {
		return nil, err
	}

	schemaJSON, err := json.Marshal(columns)
	if err != nil {
		return nil, err
	}
	schema, err := bigquery.SchemaFromJSON(schemaJSON)
	if err != nil || len(schema) == 0 {
		return nil, fmt.Errorf("could not create BigQuery schema from stream spec, err: %v", err)
	}

	md := &bigquery.TableMetadata{
		Schema: schema,
	}

	if tableSpec.TableCreation == nil {
		return md, err
	}

	md.Description = tableSpec.TableCreation.Description
	md.RequirePartitionFilter = tableSpec.TableCreation.RequirePartitionFilter

	if len(tableSpec.TableCreation.Clustering) > 0 {
		md.Clustering = &bigquery.Clustering{Fields: tableSpec.TableCreation.Clustering}
	}

	if tableSpec.TableCreation.TimePartitioning != nil {
		md.TimePartitioning = &bigquery.TimePartitioning{
			Type:       bigquery.TimePartitioningType(tableSpec.TableCreation.TimePartitioning.Type),
			Expiration: time.Duration(tableSpec.TableCreation.TimePartitioning.ExpirationHours) * time.Hour,
			Field:      tableSpec.TableCreation.TimePartitioning.Field,
		}
	}

	return md, err
}

func (l *loader) createTableMetadataColumns(tableSpec entity.Table) ([]entity.Column, error) {
	var columns []entity.Column
	for _, col := range tableSpec.Columns {
		if col.Name != "" {
			columns = append(columns, col)
			continue
		}
		if col.NameFromId != nil {
			if len(col.NameFromId.Preset) > 0 {
				for _, colName := range col.NameFromId.Preset {
					newCol := col
					newCol.Name = colName
					columns = append(columns, newCol)
				}
			}
		}
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf(l.lgprfx() + "no columns could be generated from spec")
	}
	return columns, nil
}

type RowItem struct {
	Name  string
	Value any
}

type Columns map[string]entity.Column

// createRows processes each incoming transformed event and create the BQ rows to be
// inserted according to the sink spec.
func (l *loader) createRows(data []*entity.Transformed) ([]*Row, Columns, error) {

	var (
		row     *Row
		rows    []*Row
		skipRow bool
		err     error
	)
	tableSpec := l.spec.Sink.Config.Tables[0]
	newColumns := make(Columns)

	// Each incoming Transformed map represents a possible row
	for _, rawRowData := range data {

		if skipRow {
			skipRow = false
			continue
		}

		row, skipRow, err = l.createRow(tableSpec, rawRowData, newColumns)
		if err != nil {
			return rows, nil, err
		}

		if row.Size() == 0 || skipRow {
			continue
		}

		if tableSpec.InsertIdFromId != "" {
			if insertId, ok := rawRowData.Data[tableSpec.InsertIdFromId]; ok {
				row.InsertId, ok = insertId.(string)
				if !ok {
					// This can only happen in case of a badly written stream spec or
					// an event is sent where its insert ID field is of the wrong type.
					// The event will still be inserted but the effectiveness of BQ
					// best effort deduplication is reduced. For this reason the issue
					// is notified while processing is allowed to continue.
					l.notifier.Notify(entity.NotifyLevelError, "Corrupt insert ID (%#v) in event, tableSpec: %+v", row.InsertId, tableSpec)
				}
			}
		}
		rows = append(rows, row)
	}

	return rows, newColumns, nil
}

func (l *loader) createRow(tableSpec entity.Table, rawRowData *entity.Transformed, newColumns Columns) (row *Row, skipRow bool, err error) {

	row = NewRow()
	// Try to find each row item in the incoming row data item, based on column spec
	for _, col := range tableSpec.Columns {

		if skipRow {
			break
		}

		if col.ValueFromId == entity.GeistIngestionTime {
			row.AddItem(&RowItem{
				Name:  col.Name,
				Value: time.Now().UTC(),
			})
			continue
		}

		value, ok := rawRowData.Data[col.ValueFromId]
		if !ok {
			continue
		}

		colName, err := getColumnName(col, rawRowData)
		if err != nil {
			return row, skipRow, err
		}

		if skipRow = l.shouldSkipRow(col, colName, value); skipRow {
			break
		}

		if !l.columnExists(colName) {
			newColumns[colName] = col
		}

		row.AddItem(&RowItem{
			Name:  colName,
			Value: value,
		})
	}
	return row, skipRow, err
}

func (l *loader) shouldSkipRow(col entity.Column, colName string, value any) bool {
	if colName == "" {
		// This can only happen when using the dynamic column generation option and
		// the incoming event has an empty string in the required field. This type of
		// filtering is allowed but should instead be done with a filter in the stream
		// spec. The processing is allowed to continue but the issue is notified as
		// an error.
		l.notifier.Notify(entity.NotifyLevelError, "Column name could not be created for col: %+v, event invalid and disregarded", col)
		return true
	}

	if l.spec.Sink.Config.DiscardInvalidData {
		if errValidation := validateData(col, value); errValidation != nil {
			l.notifier.Notify(entity.NotifyLevelWarn, "Invalid data found for col: %+v, err: %v, event disregarded", col, errValidation)
			return true
		}
	}
	return false
}

func validateData(col entity.Column, data any) error {

	var (
		err          error
		correctType  bool
		invalidValue bool
	)

	if col.Mode == "REQUIRED" {
		if isNil(data) {
			return fmt.Errorf("field mode set to REQUIRED but null value provided")
		}
	}
	switch col.Type {
	case string(bigquery.TimestampFieldType):
		switch data.(type) {
		case time.Time, nil:
			correctType = true
			if data.(time.Time).IsZero() {
				invalidValue = true
			}
		}
	case string(bigquery.BooleanFieldType), "BOOL":
		correctType = isBoolFieldType(data)
	case string(bigquery.IntegerFieldType), "INT64":
		correctType = isIntFieldType(data)
	case string(bigquery.StringFieldType):
		correctType = isStringFieldType(data)
	case string(bigquery.FloatFieldType), "FLOAT64", string(bigquery.NumericFieldType):
		correctType = isFloatFieldType(data)
	case string(bigquery.BytesFieldType):
		correctType = isByteFieldType(data)
	default:
		// No data validation for RECORD, array/repeated fields
		correctType = true
	}
	if !correctType {
		err = fmt.Errorf("field type in schema: %s, does not match actual type: %T", col.Type, data)
	} else if invalidValue {
		err = fmt.Errorf("invalid field value: %v", data)
	}
	return err
}

func isBoolFieldType(d any) (r bool) {
	switch d.(type) {
	case bool, nil:
		r = true
	}
	return
}

func isIntFieldType(d any) (r bool) {
	switch d.(type) {
	case int, int32, int64, nil:
		r = true
	}
	return
}

func isStringFieldType(d any) (r bool) {
	switch d.(type) {
	case string, nil:
		r = true
	}
	return
}

func isFloatFieldType(d any) (r bool) {
	switch d.(type) {
	case float64, float32, int, int32, int64, nil:
		r = true
	}
	return
}

func isByteFieldType(d any) (r bool) {
	switch d.(type) {
	case []byte, nil:
		r = true
	}
	return
}

func (l *loader) handleDynamicColumnUpdates(ctx context.Context, newColumns Columns) error {

	if len(newColumns) == 0 {
		return nil
	}

	l.notifier.Notify(entity.NotifyLevelInfo, "New columns found, to be created: %+v", newColumns)

	var newBqColumns bigquery.Schema

	for colName, col := range newColumns {
		newBqColumns = append(newBqColumns, &bigquery.FieldSchema{
			Name:        colName,
			Type:        bigquery.FieldType(col.Type),
			Description: col.Description,
			Repeated:    col.Mode == "REPEATED",
			// Required must be false for columns appended to a table

			// TODO: possibly add support for other options here later
		})
	}

	return l.addColumnsToTable(ctx, newBqColumns)
}

func (l *loader) addColumnsToTable(ctx context.Context, newColumns bigquery.Schema) error {

	l.mdMutex.Lock()
	defer l.mdMutex.Unlock()

	// We cannot use the already stored metadata in the Loader since we need to get the etag from BQ
	// to ensure consistency in the Update operation.
	meta, _, err := l.client.GetTableMetadata(ctx, l.table)
	if err != nil {
		return err
	}

	update := bigquery.TableMetadataToUpdate{
		Schema: append(meta.Schema, newColumns...),
	}

	tm, err := l.client.UpdateTable(ctx, l.table, update, meta.ETag)

	if err == nil {
		// BQ takes a while to allow ingestion with new schema, this sleep will reduce number of retries,
		// although not required for actual functionality.
		if !sleepCtx(ctx, TableUpdateBackoffTime) {
			err = entity.ErrEntityShutdownRequested
		}
		l.metadata = tm
	}
	return err
}

func (l *loader) columnExists(colName string) bool {

	if l.metadata != nil {
		for _, field := range l.metadata.Schema {
			if field.Name == colName {
				return true
			}
		}
	}
	return false
}

func getColumnName(col entity.Column, rawRowData *entity.Transformed) (string, error) {
	if col.Name != "" {
		return col.Name, nil
	}
	if col.NameFromId == nil {
		return "", fmt.Errorf("one of Name or NameFromId need to be present in column spec, col spec: %+v", col)
	}

	if value, ok := rawRowData.Data[col.NameFromId.SuffixFromId]; ok {

		v, ok := value.(string)
		if !ok {
			return "", fmt.Errorf("invalid type in stream spec for col: %+v, transformed data: %s", col, rawRowData)
		}
		return col.NameFromId.Prefix + v, nil
	}
	return "", fmt.Errorf("column name not found, col spec: %+v, transformed data: %s", col, rawRowData)
}

func (l *loader) insertRows(ctx context.Context, rows []*Row) error {

	return l.inserter.Put(ctx, rows)
}

func (l *loader) lgprfx() string {
	return "[xbigquery.loader:" + l.id + "] "
}

type Row struct {
	InsertId string
	rowItems map[string]bigquery.Value
}

func NewRow() *Row {
	return &Row{
		rowItems: make(map[string]bigquery.Value),
	}
}

func (r *Row) AddItem(item *RowItem) {
	r.rowItems[item.Name] = item.Value
}

// Save is required for implementing the BigQuery ValueSaver interface, as used by the bigquery.Inserter
func (r *Row) Save() (map[string]bigquery.Value, string, error) {
	return r.rowItems, r.InsertId, nil
}

func (r *Row) Size() int {
	return len(r.rowItems)
}
