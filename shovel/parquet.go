package shovel

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

// ParquetShovel converts between parquet binary format and CSV for editing.
// It preserves schema information between CopyIn and CopyOut operations to maintain
// data type consistency during round-trip conversions.
type ParquetShovel struct {
	// Schema holds the parquet schema extracted during CopyIn for reuse in CopyOut
	Schema *parquetSchema
	// Metadata holds the key-value metadata from the original parquet file
	Metadata []*parquet.KeyValue
}

// parquetSchema holds the schema information for a parquet file
type parquetSchema struct {
	Fields []parquetField
}

// parquetField represents a field in the parquet schema
type parquetField struct {
	Name          string
	Type          string
	ConvertedType *parquet.ConvertedType
	LogicalType   *parquet.LogicalType
}

// CopyIn converts parquet data to CSV format for editing.
// It extracts and stores the parquet schema for later use in CopyOut.
func (p *ParquetShovel) CopyIn(dst io.WriteCloser, src io.ReadCloser) error {
	defer src.Close()

	// Read all parquet data into buffer
	parquetData, err := io.ReadAll(src)
	if err != nil {
		return fmt.Errorf("failed to read parquet data: %w", err)
	}

	// Create buffer reader for parquet data
	fr := buffer.NewBufferFileFromBytes(parquetData)

	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		return fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pr.ReadStop()

	// Extract schema information
	schema, err := extractSchema(pr)
	if err != nil {
		return fmt.Errorf("failed to extract schema: %w", err)
	}

	// Store schema in shovel state
	p.Schema = schema

	// Extract and store metadata for preservation
	p.Metadata = pr.Footer.KeyValueMetadata

	// Create CSV writer
	csvWriter := csv.NewWriter(dst)
	defer csvWriter.Flush()

	// Use schema to determine headers
	headers := make([]string, len(schema.Fields))
	for i, field := range schema.Fields {
		headers[i] = field.Name
	}

	// Write CSV header
	if err := csvWriter.Write(headers); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Read and write all records
	num := int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		records, err := pr.ReadByNumber(1)
		if err != nil {
			return fmt.Errorf("failed to read parquet record: %w", err)
		}

		if len(records) > 0 {
			recordMap, err := extractFieldValues(records[0], schema)
			if err != nil {
				return fmt.Errorf("failed to extract field values: %w", err)
			}

			if err := writeRecordAsCSV(csvWriter, recordMap, headers, schema); err != nil {
				return fmt.Errorf("failed to write CSV record: %w", err)
			}
		}
	}

	return nil
}

// CopyOut converts CSV back to parquet format.
// Uses stored schema if available, otherwise infers schema from CSV data with type widening.
func (p *ParquetShovel) CopyOut(dst io.WriteCloser, src io.ReadCloser) error {
	defer dst.Close()

	// Parse CSV from source
	csvReader := csv.NewReader(src)

	// Read header row
	headers, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %w", err)
	}

	// Read all records
	csvRecords, err := csvReader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read CSV records: %w", err)
	}

	if len(csvRecords) == 0 {
		return fmt.Errorf("no records found in CSV")
	}

	// Convert CSV records to maps
	records := make([]map[string]interface{}, 0, len(csvRecords))
	for _, csvRecord := range csvRecords {
		if len(csvRecord) != len(headers) {
			continue // Skip malformed rows
		}
		record := make(map[string]interface{})
		for i, value := range csvRecord {
			record[headers[i]] = parseCSVValue(value)
		}
		records = append(records, record)
	}

	// Create buffer writer for parquet data
	fw := buffer.NewBufferFile()

	// Use stored schema if available, otherwise infer from data
	var schemaToUse *parquetSchema
	if p.Schema != nil {
		schemaToUse = p.Schema
	} else {
		// Infer schema from all records using type widening, preserving header order
		inferredSchema, err := inferSchemaWithTypeWidening(records, headers)
		if err != nil {
			return fmt.Errorf("failed to infer schema: %w", err)
		}
		schemaToUse = inferredSchema
	}

	// Create struct type for parquet writer based on schema
	structType, err := createStructTypeFromSchema(schemaToUse)
	if err != nil {
		return fmt.Errorf("failed to create struct type: %w", err)
	}

	// Create a sample struct instance for the writer
	sampleStruct := reflect.New(structType).Interface()

	pw, err := writer.NewParquetWriter(fw, sampleStruct, 4)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}

	// Write records by converting maps to structs
	for rowIndex, record := range records {
		structRecord, err := convertMapToStruct(record, structType, schemaToUse, rowIndex+1) // +1 for 1-based row numbering
		if err != nil {
			return err // Pass through the detailed error message directly
		}

		if err := pw.Write(structRecord); err != nil {
			return fmt.Errorf("failed to write parquet record at row %d: %w", rowIndex+1, err)
		}
	}

	// Restore metadata if we have it (need to flush first)
	if err := pw.Flush(true); err != nil {
		return fmt.Errorf("failed to flush parquet writer: %w", err)
	}

	// Restore preserved metadata to maintain pandas compatibility
	if p.Metadata != nil {
		pw.Footer.KeyValueMetadata = p.Metadata
	}

	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to stop parquet writer: %w", err)
	}
	fw.Close()

	// Copy the written parquet data to destination
	parquetData := fw.Bytes()
	if _, err := io.Copy(dst, bytes.NewReader(parquetData)); err != nil {
		return fmt.Errorf("failed to copy parquet data: %w", err)
	}

	return nil
}

// Helper functions

// writeRecordAsCSV writes a record map as a CSV row using the provided headers order
func writeRecordAsCSV(csvWriter *csv.Writer, record map[string]interface{}, headers []string, schema *parquetSchema) error {
	values := make([]string, len(headers))
	for i, header := range headers {
		if value, exists := record[header]; exists {
			// Find the corresponding schema field for type information
			var field *parquetField
			for _, f := range schema.Fields {
				if f.Name == header {
					field = &f
					break
				}
			}
			values[i] = formatCSVValue(value, field)
		} else {
			values[i] = ""
		}
	}
	return csvWriter.Write(values)
}

// formatCSVValue converts a value to its string representation for CSV output
func formatCSVValue(value interface{}, field *parquetField) string {
	if value == nil {
		return ""
	}

	// Handle date and datetime formatting based on schema information
	if field != nil {
		// Handle DATE type (days since epoch)
		if field.ConvertedType != nil && *field.ConvertedType == parquet.ConvertedType_DATE {
			if days, ok := value.(int32); ok {
				// Convert days since Unix epoch to date
				epochDate := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
				date := epochDate.AddDate(0, 0, int(days))
				return date.Format("2006-01-02")
			}
		}

		// Handle TIMESTAMP type (nanoseconds since epoch)
		if field.LogicalType != nil && field.LogicalType.TIMESTAMP != nil {
			if nanos, ok := value.(int64); ok {
				// Convert nanoseconds since Unix epoch to timestamp
				timestamp := time.Unix(0, nanos).UTC()
				return timestamp.Format("2006-01-02 15:04:05.000000000")
			}
		}
	}

	// Default formatting for other types
	switch v := value.(type) {
	case string:
		return v
	case int, int32, int64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%g", v)
	case bool:
		return strconv.FormatBool(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// parseCSVValue attempts to parse a CSV string value into the most appropriate Go type
func parseCSVValue(value string) interface{} {
	if value == "" {
		return nil
	}

	// Try to parse as integer
	if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
		return intVal
	}

	// Try to parse as float
	if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
		return floatVal
	}

	// Try to parse as boolean
	if boolVal, err := strconv.ParseBool(value); err == nil {
		return boolVal
	}

	// Default to string
	return value
}

// extractSchema extracts the schema information from a parquet reader
func extractSchema(pr *reader.ParquetReader) (*parquetSchema, error) {
	// Get the schema tree from the parquet reader
	schemaTree := pr.SchemaHandler.SchemaElements

	schema := &parquetSchema{
		Fields: make([]parquetField, 0),
	}

	// Skip the first element which is the root schema
	for i := 1; i < len(schemaTree); i++ {
		element := schemaTree[i]
		// Only include elements that have a type (leaf nodes, not groups)
		if element.Name != "" && element.Type != nil {
			fieldType := getParquetTypeString(element)
			// Skip if we get an empty or invalid type
			if fieldType != "" && fieldType != "unknown" {
				var fieldName string
				// Try to get external name (original) first, fallback to element name
				exName := pr.SchemaHandler.GetExName(i)
				if exName != "" {
					fieldName = exName
				} else {
					fieldName = element.Name
				}

				field := parquetField{
					Name:          fieldName,
					Type:          fieldType,
					ConvertedType: element.ConvertedType,
					LogicalType:   element.LogicalType,
				}
				schema.Fields = append(schema.Fields, field)
			}
		}
	}

	return schema, nil
}

// getParquetTypeString converts parquet type to string representation
func getParquetTypeString(element *parquet.SchemaElement) string {
	if element.Type != nil {
		return element.Type.String()
	}
	// If no type specified, it might be a group/container - skip it
	// or default to string type
	return "BYTE_ARRAY"
}

// extractFieldValues uses reflection to extract field values from a struct
func extractFieldValues(record interface{}, schema *parquetSchema) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	// Use reflection to get the struct value
	val := reflect.ValueOf(record)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, got %v", val.Kind())
	}

	// Match struct fields with schema fields by index (they should correspond)
	for i := 0; i < val.NumField() && i < len(schema.Fields); i++ {
		field := val.Field(i)
		schemaField := schema.Fields[i]

		// Use the original schema field name, not the normalized struct field name
		fieldName := schemaField.Name

		// Extract the actual value
		if field.CanInterface() {
			fieldValue := field.Interface()

			// Dereference pointers
			if field.Kind() == reflect.Ptr && !field.IsNil() {
				fieldValue = field.Elem().Interface()
			}

			result[fieldName] = fieldValue
		}
	}

	return result, nil
}

// parquetTypeRank represents the hierarchy of types for widening
type parquetTypeRank int

const (
	typeEmpty parquetTypeRank = iota
	typeBoolean
	typeInt
	typeFloat
	typeString
)

// inferSchemaWithTypeWidening analyzes all records using type widening approach
func inferSchemaWithTypeWidening(records []map[string]interface{}, headers []string) (*parquetSchema, error) {
	if len(records) == 0 {
		return &parquetSchema{Fields: []parquetField{}}, nil
	}

	// Use provided headers to preserve field order
	fieldNames := headers

	// For each field, determine the widest type needed
	schema := &parquetSchema{
		Fields: make([]parquetField, len(fieldNames)),
	}

	for i, fieldName := range fieldNames {
		widenedType := determineWidestType(fieldName, records)
		schema.Fields[i] = parquetField{
			Name: fieldName,
			Type: widenedType,
		}
	}

	return schema, nil
}

// determineWidestType examines all values for a field and returns the widest type needed
func determineWidestType(fieldName string, records []map[string]interface{}) string {
	currentTypeRank := typeEmpty

	// Single pass through all values for this field
	for _, record := range records {
		value, exists := record[fieldName]
		if !exists {
			continue
		}

		valueTypeRank := getValueTypeRank(value)

		// Widen type if necessary
		if valueTypeRank > currentTypeRank {
			currentTypeRank = valueTypeRank
		}

		// Early exit if we've reached string type (widest)
		if currentTypeRank == typeString {
			break
		}
	}

	// Convert type rank back to parquet type string
	return typeRankToParquetType(currentTypeRank)
}

// getValueTypeRank determines the type rank of a single value
func getValueTypeRank(value interface{}) parquetTypeRank {
	if value == nil {
		return typeEmpty
	}

	// Handle string values from CSV
	if str, ok := value.(string); ok {
		if str == "" {
			return typeEmpty
		}

		// Try to parse as different types in order of specificity
		// Try boolean first
		if _, err := strconv.ParseBool(str); err == nil {
			return typeBoolean
		}

		// Try integer
		if _, err := strconv.ParseInt(str, 10, 64); err == nil {
			return typeInt
		}

		// Try float
		if _, err := strconv.ParseFloat(str, 64); err == nil {
			return typeFloat
		}

		// If none of the above, it's a string
		return typeString
	}

	// Handle direct Go types (shouldn't happen much in CSV context, but just in case)
	switch value.(type) {
	case bool:
		return typeBoolean
	case int, int32, int64:
		return typeInt
	case float32, float64:
		return typeFloat
	default:
		return typeString
	}
}

// typeRankToParquetType converts a type rank to parquet type string
func typeRankToParquetType(rank parquetTypeRank) string {
	switch rank {
	case typeEmpty:
		return "BYTE_ARRAY" // Default to string for empty fields
	case typeBoolean:
		return "BOOLEAN"
	case typeInt:
		return "INT64"
	case typeFloat:
		return "DOUBLE"
	case typeString:
		return "BYTE_ARRAY"
	default:
		return "BYTE_ARRAY"
	}
}

// createStructTypeFromSchema dynamically creates a struct type based on the schema
func createStructTypeFromSchema(schema *parquetSchema) (reflect.Type, error) {
	fields := make([]reflect.StructField, len(schema.Fields))

	for i, schemaField := range schema.Fields {
		fieldType, err := parquetTypeToGoType(schemaField.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to convert type for field %s: %w", schemaField.Name, err)
		}

		// Create proper parquet tag with type information including logical types
		parquetType := getParquetTagTypeWithLogical(schemaField)
		if parquetType == "" {
			return nil, fmt.Errorf("empty parquet type for field %s", schemaField.Name)
		}
		tag := fmt.Sprintf(`parquet:"name=%s, %s"`, schemaField.Name, parquetType)

		fields[i] = reflect.StructField{
			Name: normalizeFieldName(schemaField.Name),
			Type: fieldType,
			Tag:  reflect.StructTag(tag),
		}
	}

	return reflect.StructOf(fields), nil
}

// getParquetTagType converts parquet type to the tag format expected by parquet-go
func getParquetTagType(parquetType string) string {
	switch strings.ToUpper(parquetType) {
	case "BOOLEAN":
		return "BOOLEAN"
	case "INT32":
		return "INT32"
	case "INT64":
		return "INT64"
	case "FLOAT":
		return "FLOAT"
	case "DOUBLE":
		return "DOUBLE"
	case "BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY":
		return "BYTE_ARRAY, convertedtype=UTF8"
	default:
		// Default to UTF8 string
		return "BYTE_ARRAY, convertedtype=UTF8"
	}
}

// getParquetTagTypeWithLogical converts parquet field to complete tag format including logical types
func getParquetTagTypeWithLogical(field parquetField) string {
	baseType := strings.ToUpper(field.Type)

	// Handle logical types (takes precedence over converted types)
	if field.LogicalType != nil {
		if field.LogicalType.TIMESTAMP != nil {
			// For timestamp logical type
			return fmt.Sprintf("type=%s, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=NANOS", baseType)
		}
		if field.LogicalType.DATE != nil {
			// For date logical type
			return fmt.Sprintf("type=%s, logicaltype=DATE", baseType)
		}
	}

	// Handle converted types
	if field.ConvertedType != nil {
		switch *field.ConvertedType {
		case parquet.ConvertedType_DATE:
			return fmt.Sprintf("type=%s, convertedtype=DATE", baseType)
		case parquet.ConvertedType_UTF8:
			return fmt.Sprintf("type=%s, convertedtype=UTF8", baseType)
		}
	}

	// Fall back to basic type
	switch baseType {
	case "BOOLEAN":
		return "type=BOOLEAN"
	case "INT32":
		return "type=INT32"
	case "INT64":
		return "type=INT64"
	case "FLOAT":
		return "type=FLOAT"
	case "DOUBLE":
		return "type=DOUBLE"
	case "BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY":
		return "type=BYTE_ARRAY, convertedtype=UTF8"
	default:
		return "type=BYTE_ARRAY, convertedtype=UTF8"
	}
}

// parquetTypeToGoType converts parquet type strings to Go types
func parquetTypeToGoType(parquetType string) (reflect.Type, error) {
	switch strings.ToUpper(parquetType) {
	case "BOOLEAN":
		return reflect.TypeOf(false), nil
	case "INT32":
		return reflect.TypeOf(int32(0)), nil
	case "INT64":
		return reflect.TypeOf(int64(0)), nil
	case "FLOAT":
		return reflect.TypeOf(float32(0)), nil
	case "DOUBLE":
		return reflect.TypeOf(float64(0)), nil
	case "BYTE_ARRAY", "FIXED_LEN_BYTE_ARRAY":
		return reflect.TypeOf(""), nil
	default:
		// Default to string for unknown types
		return reflect.TypeOf(""), nil
	}
}

// normalizeFieldName ensures field names are valid Go identifiers
func normalizeFieldName(name string) string {
	// Replace dots, hyphens, and other special characters with underscores
	result := strings.ReplaceAll(name, ".", "")
	result = strings.ReplaceAll(result, "-", "")
	result = strings.ReplaceAll(result, " ", "")

	// Convert to title case
	if len(result) > 0 {
		result = strings.ToUpper(string(result[0])) + result[1:]
	}

	// Ensure first character is a letter
	if len(result) > 0 && !((result[0] >= 'A' && result[0] <= 'Z') || (result[0] >= 'a' && result[0] <= 'z')) {
		result = "Field" + result
	}

	// If empty or still invalid, use a default
	if result == "" {
		result = "Field"
	}

	return result
}

// convertMapToStruct converts a map to a struct instance based on the provided type and schema
func convertMapToStruct(record map[string]interface{}, structType reflect.Type, schema *parquetSchema, rowNumber int) (interface{}, error) {
	structValue := reflect.New(structType).Elem()

	for i, field := range schema.Fields {
		if i >= structType.NumField() {
			continue
		}

		fieldValue := structValue.Field(i)

		if !fieldValue.CanSet() {
			continue
		}

		// Get value from map
		mapValue, exists := record[field.Name]
		if !exists {
			// Set zero value for missing fields
			continue
		}

		// Convert and set the value with schema-aware parsing
		if err := setFieldValue(fieldValue, mapValue, &field); err != nil {
			return nil, fmt.Errorf("field '%s' at row %d: cannot convert %q to %s",
				field.Name, rowNumber, fmt.Sprintf("%v", mapValue), fieldValue.Type())
		}
	}

	return structValue.Interface(), nil
}

// setFieldValue sets a reflect.Value with proper type conversion
func setFieldValue(fieldValue reflect.Value, value interface{}, field *parquetField) error {
	if value == nil {
		return nil // Leave as zero value
	}

	targetType := fieldValue.Type()
	sourceValue := reflect.ValueOf(value)

	// Handle exact type matches first
	if sourceValue.Type() == targetType {
		fieldValue.Set(sourceValue)
		return nil
	}

	// Handle date/timestamp parsing if we have schema information
	if field != nil && targetType.Kind() == reflect.Int32 && field.ConvertedType != nil && *field.ConvertedType == parquet.ConvertedType_DATE {
		// Parse date string back to days since epoch
		if dateStr, ok := value.(string); ok {
			if parsedDate, err := time.Parse("2006-01-02", dateStr); err == nil {
				epochDate := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
				days := int32(parsedDate.Sub(epochDate).Hours() / 24)
				fieldValue.SetInt(int64(days))
				return nil
			}
		}
	}

	if field != nil && targetType.Kind() == reflect.Int64 && field.LogicalType != nil && field.LogicalType.TIMESTAMP != nil {
		// Parse timestamp string back to nanoseconds since epoch
		if timestampStr, ok := value.(string); ok {
			if parsedTime, err := time.Parse("2006-01-02 15:04:05.000000000", timestampStr); err == nil {
				nanos := parsedTime.UnixNano()
				fieldValue.SetInt(nanos)
				return nil
			}
		}
	}

	// Handle string conversions - this is critical for type widening
	// When we widen types to string, we need to convert the actual value to its string representation
	if targetType.Kind() == reflect.String {
		fieldValue.SetString(fmt.Sprintf("%v", value))
		return nil
	}

	// Handle numeric conversions for type widening
	switch targetType.Kind() {
	case reflect.Int32:
		if v, ok := convertToInt64(value); ok {
			fieldValue.SetInt(v)
			return nil
		}
	case reflect.Int64:
		if v, ok := convertToInt64(value); ok {
			fieldValue.SetInt(v)
			return nil
		}
	case reflect.Float32:
		if v, ok := convertToFloat64(value); ok {
			fieldValue.SetFloat(v)
			return nil
		}
	case reflect.Float64:
		if v, ok := convertToFloat64(value); ok {
			fieldValue.SetFloat(v)
			return nil
		}
	case reflect.Bool:
		if v, ok := convertToBool(value); ok {
			fieldValue.SetBool(v)
			return nil
		}
	}

	// Try to convert compatible types as fallback
	if sourceValue.Type().ConvertibleTo(targetType) {
		fieldValue.Set(sourceValue.Convert(targetType))
		return nil
	}

	return fmt.Errorf("cannot convert %T to %s", value, targetType)
}

// Helper functions for type conversions

func convertToInt64(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case float32:
		return int64(v), true
	case float64:
		return int64(v), true
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i, true
		}
	}
	return 0, false
}

// convertToFloat64 attempts to convert a value to float64
func convertToFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// convertToBool attempts to convert a value to bool
func convertToBool(value interface{}) (bool, bool) {
	switch v := value.(type) {
	case bool:
		return v, true
	case int:
		return v != 0, true
	case int32:
		return v != 0, true
	case int64:
		return v != 0, true
	case string:
		if b, err := strconv.ParseBool(v); err == nil {
			return b, true
		}
	}
	return false, false
}
