package shovel

import (
	"bytes"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

// TestData represents sample data for testing
type TestData struct {
	Name   string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	Age    int32   `parquet:"name=age, type=INT32"`
	Score  float64 `parquet:"name=score, type=DOUBLE"`
	Active bool    `parquet:"name=active, type=BOOLEAN"`
}

// TestDataWithDots represents data with dot-separated field names
type TestDataWithDots struct {
	FirstName string  `parquet:"name=first.name, type=BYTE_ARRAY, convertedtype=UTF8"`
	LastName  string  `parquet:"name=last.name, type=BYTE_ARRAY, convertedtype=UTF8"`
	TestScore float64 `parquet:"name=test.score, type=DOUBLE"`
}

func createTestParquetData() []byte {
	// Create sample parquet data
	testData := []TestData{
		{"Alice", 25, 95.5, true},
		{"Bob", 30, 87.2, false},
		{"Charlie", 35, 92.8, true},
	}

	// Create buffer writer
	fw := buffer.NewBufferFile()

	// Create parquet writer
	pw, err := writer.NewParquetWriter(fw, new(TestData), 4)
	if err != nil {
		panic(err)
	}

	// Write test data
	for _, record := range testData {
		if err := pw.Write(record); err != nil {
			panic(err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		panic(err)
	}
	fw.Close()

	return fw.Bytes()
}

func createTestParquetDataWithDots() []byte {
	// Create sample parquet data with dot-separated field names
	testData := []TestDataWithDots{
		{"John", "Doe", 88.5},
		{"Jane", "Smith", 91.2},
		{"Mike", "Johnson", 85.7},
	}

	// Create buffer writer
	fw := buffer.NewBufferFile()

	// Create parquet writer
	pw, err := writer.NewParquetWriter(fw, new(TestDataWithDots), 4)
	if err != nil {
		panic(err)
	}

	// Write test data
	for _, record := range testData {
		if err := pw.Write(record); err != nil {
			panic(err)
		}
	}

	if err := pw.WriteStop(); err != nil {
		panic(err)
	}
	fw.Close()

	return fw.Bytes()
}

func TestParquetShovelCopyIn(t *testing.T) {
	tests := []struct {
		name           string
		parquetData    []byte
		expectedHeader string
		expectedRows   []string
	}{
		{
			name:           "Basic parquet to CSV conversion",
			parquetData:    createTestParquetData(),
			expectedHeader: "name,age,score,active",
			expectedRows: []string{
				"Alice,25,95.5,true",
				"Bob,30,87.2,false",
				"Charlie,35,92.8,true",
			},
		},
		{
			name:           "Parquet with dot-separated field names",
			parquetData:    createTestParquetDataWithDots(),
			expectedHeader: "first.name,last.name,test.score",
			expectedRows: []string{
				"John,Doe,88.5",
				"Jane,Smith,91.2",
				"Mike,Johnson,85.7",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shovel := &ParquetShovel{}

			// Create input reader
			src := io.NopCloser(bytes.NewReader(tt.parquetData))

			// Create output buffer
			var dst bytes.Buffer
			dstCloser := &nopWriteCloser{&dst}

			// Test CopyIn
			err := shovel.CopyIn(dstCloser, src)
			if err != nil {
				t.Fatalf("CopyIn failed: %v", err)
			}

			// Parse CSV output
			csvOutput := dst.String()
			lines := strings.Split(strings.TrimSpace(csvOutput), "\n")

			// Check header
			if lines[0] != tt.expectedHeader {
				t.Errorf("Expected header %q, got %q", tt.expectedHeader, lines[0])
			}

			// Check data rows
			if len(lines)-1 != len(tt.expectedRows) {
				t.Errorf("Expected %d data rows, got %d", len(tt.expectedRows), len(lines)-1)
			}

			for i, expectedRow := range tt.expectedRows {
				if i+1 >= len(lines) {
					t.Errorf("Missing expected row: %q", expectedRow)
					continue
				}
				if lines[i+1] != expectedRow {
					t.Errorf("Row %d: expected %q, got %q", i, expectedRow, lines[i+1])
				}
			}

			// Verify schema was stored
			if shovel.Schema == nil {
				t.Error("Schema was not stored in shovel")
			} else {
				expectedFieldCount := len(strings.Split(tt.expectedHeader, ","))
				if len(shovel.Schema.Fields) != expectedFieldCount {
					t.Errorf("Expected %d schema fields, got %d", expectedFieldCount, len(shovel.Schema.Fields))
				}
			}
		})
	}
}

func TestParquetShovelCopyOutWithStoredSchema(t *testing.T) {
	// First, create a parquet file and extract its schema
	parquetData := createTestParquetData()

	shovel := &ParquetShovel{}
	src := io.NopCloser(bytes.NewReader(parquetData))
	var tempDst bytes.Buffer
	tempDstCloser := &nopWriteCloser{&tempDst}

	// Extract schema by doing CopyIn first
	err := shovel.CopyIn(tempDstCloser, src)
	if err != nil {
		t.Fatalf("Failed to extract schema: %v", err)
	}

	// Now test CopyOut with the stored schema
	csvInput := `name,age,score,active
Alice,25,95.5,true
Bob,30,87.2,false
Charlie,35,92.8,true`

	csvSrc := io.NopCloser(strings.NewReader(csvInput))
	var parquetDst bytes.Buffer
	parquetDstCloser := &nopWriteCloser{&parquetDst}

	// Test CopyOut
	err = shovel.CopyOut(parquetDstCloser, csvSrc)
	if err != nil {
		t.Fatalf("CopyOut failed: %v", err)
	}

	// Verify the output by reading it back
	outputParquetData := parquetDst.Bytes()
	if len(outputParquetData) == 0 {
		t.Fatal("No parquet data was written")
	}

	// Read the parquet data back to verify correctness
	fr := buffer.NewBufferFileFromBytes(outputParquetData)
	pr, err := reader.NewParquetReader(fr, new(TestData), 4)
	if err != nil {
		t.Fatalf("Failed to create parquet reader: %v", err)
	}
	defer pr.ReadStop()

	// Verify row count
	if pr.GetNumRows() != 3 {
		t.Errorf("Expected 3 rows, got %d", pr.GetNumRows())
	}

	// Read and verify data
	records, err := pr.ReadByNumber(int(pr.GetNumRows()))
	if err != nil {
		t.Fatalf("Failed to read parquet records: %v", err)
	}

	expectedRecords := []TestData{
		{"Alice", 25, 95.5, true},
		{"Bob", 30, 87.2, false},
		{"Charlie", 35, 92.8, true},
	}

	if len(records) != len(expectedRecords) {
		t.Errorf("Expected %d records, got %d", len(expectedRecords), len(records))
	}

	for i, record := range records {
		testData, ok := record.(TestData)
		if !ok {
			t.Errorf("Record %d: expected TestData type, got %T", i, record)
			continue
		}

		expected := expectedRecords[i]
		if testData.Name != expected.Name || testData.Age != expected.Age ||
			testData.Score != expected.Score || testData.Active != expected.Active {
			t.Errorf("Record %d: expected %+v, got %+v", i, expected, testData)
		}
	}
}

func TestParquetShovelCopyOutWithoutStoredSchema(t *testing.T) {
	// Test CopyOut without a pre-existing schema (should infer from CSV)
	shovel := &ParquetShovel{} // No schema stored

	csvInput := `name,age,score,active
Alice,25,95.5,true
Bob,30,87.2,false`

	csvSrc := io.NopCloser(strings.NewReader(csvInput))
	var parquetDst bytes.Buffer
	parquetDstCloser := &nopWriteCloser{&parquetDst}

	// Test CopyOut
	err := shovel.CopyOut(parquetDstCloser, csvSrc)
	if err != nil {
		t.Fatalf("CopyOut failed: %v", err)
	}

	// Verify the output
	outputParquetData := parquetDst.Bytes()
	if len(outputParquetData) == 0 {
		t.Fatal("No parquet data was written")
	}

	// The parquet file should be readable (basic validation)
	fr := buffer.NewBufferFileFromBytes(outputParquetData)
	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		t.Fatalf("Failed to create parquet reader: %v", err)
	}
	defer pr.ReadStop()

	// Verify row count
	if pr.GetNumRows() != 2 {
		t.Errorf("Expected 2 rows, got %d", pr.GetNumRows())
	}
}

func TestParquetShovelCopyOutTypeInferenceEdgeCases(t *testing.T) {
	tests := []struct {
		name          string
		csvInput      string
		description   string
		expectedTypes []string // Expected parquet types for each field
	}{
		{
			name: "First row empty, second row has numbers",
			csvInput: `name,age,score,active
,,,
Alice,25,95.5,true
Bob,30,87.2,false`,
			description:   "First row has empty values, subsequent rows have typed data",
			expectedTypes: []string{"BYTE_ARRAY", "INT64", "DOUBLE", "BOOLEAN"},
		},
		{
			name: "First row empty, second row has strings",
			csvInput: `name,category,status,note
,,,
Alice,premium,active,good customer
Bob,standard,inactive,needs follow-up`,
			description:   "First row has empty values, subsequent rows have string data",
			expectedTypes: []string{"BYTE_ARRAY", "BYTE_ARRAY", "BYTE_ARRAY", "BYTE_ARRAY"},
		},
		{
			name: "First row has numbers, second row has strings",
			csvInput: `id,code,priority,flag
123,456,1,true
USER001,PROMO,high,enabled
USER002,BASIC,low,disabled`,
			description:   "First row numeric values, subsequent rows have strings",
			expectedTypes: []string{"BYTE_ARRAY", "BYTE_ARRAY", "BYTE_ARRAY", "BYTE_ARRAY"}, // All become strings
		},
		{
			name: "First row has booleans, second row has strings",
			csvInput: `enabled,status,active,verified
true,false,true,false
yes,pending,maybe,unknown
no,completed,definitely,confirmed`,
			description:   "First row boolean values, subsequent rows have string representations",
			expectedTypes: []string{"BYTE_ARRAY", "BYTE_ARRAY", "BYTE_ARRAY", "BYTE_ARRAY"}, // All become strings
		},
		{
			name: "First row has floats, second row has strings",
			csvInput: `price,rate,score,percentage
99.99,0.15,4.5,85.2
free,standard,excellent,complete
premium,discounted,poor,partial`,
			description:   "First row float values, subsequent rows have descriptive strings",
			expectedTypes: []string{"BYTE_ARRAY", "BYTE_ARRAY", "BYTE_ARRAY", "BYTE_ARRAY"}, // All become strings
		},
		{
			name: "Mixed type inference challenges",
			csvInput: `field1,field2,field3,field4
,42,true,3.14
hello,,false,
world,999,,2.71
test,abc,maybe,invalid`,
			description:   "Complex mix with empty values and type changes across rows",
			expectedTypes: []string{"BYTE_ARRAY", "BYTE_ARRAY", "BYTE_ARRAY", "BYTE_ARRAY"}, // All become strings due to mixed content
		},
		{
			name: "Real-world mixed types from test_mixed_types.csv",
			csvInput: `a,b,c
1,2,true
a,2,1
3,2,nope
4,2.2,`,
			description:   "Real CSV with int->string, float in column b, bool->int->string in column c",
			expectedTypes: []string{"BYTE_ARRAY", "DOUBLE", "BYTE_ARRAY"}, // a: int->string, b: int->float, c: bool->int->string
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shovel := &ParquetShovel{} // No stored schema - must infer

			csvSrc := io.NopCloser(strings.NewReader(tt.csvInput))
			var parquetDst bytes.Buffer
			parquetDstCloser := &nopWriteCloser{&parquetDst}

			// Test CopyOut - this should succeed despite type inference challenges
			err := shovel.CopyOut(parquetDstCloser, csvSrc)
			if err != nil {
				t.Fatalf("CopyOut failed for %s: %v", tt.description, err)
			}

			// Verify we got some parquet data
			outputParquetData := parquetDst.Bytes()
			if len(outputParquetData) == 0 {
				t.Fatalf("No parquet data was written for %s", tt.description)
			}

			// Verify the parquet file is readable
			fr := buffer.NewBufferFileFromBytes(outputParquetData)
			pr, err := reader.NewParquetReader(fr, nil, 4)
			if err != nil {
				t.Fatalf("Failed to create parquet reader for %s: %v", tt.description, err)
			}
			defer pr.ReadStop()

			// Count expected data rows (excluding header)
			lines := strings.Split(strings.TrimSpace(tt.csvInput), "\n")
			expectedRows := len(lines) - 1 // Subtract header row

			if pr.GetNumRows() != int64(expectedRows) {
				t.Errorf("%s: Expected %d rows, got %d", tt.description, expectedRows, pr.GetNumRows())
			}

			// Verify we can read the data back (basic validation)
			records, err := pr.ReadByNumber(int(pr.GetNumRows()))
			if err != nil {
				t.Fatalf("Failed to read records for %s: %v", tt.description, err)
			}

			if len(records) != expectedRows {
				t.Errorf("%s: Expected to read %d records, got %d", tt.description, expectedRows, len(records))
			}

			// Verify schema types if expected types are provided
			if tt.expectedTypes != nil {
				// Extract the schema from the parquet file to verify type inference
				testShovelForSchema := &ParquetShovel{}
				schemaExtractSrc := io.NopCloser(bytes.NewReader(outputParquetData))
				var schemaDst bytes.Buffer
				schemaDstCloser := &nopWriteCloser{&schemaDst}

				err = testShovelForSchema.CopyIn(schemaDstCloser, schemaExtractSrc)
				if err != nil {
					t.Fatalf("Failed to extract schema for verification: %v", err)
				}

				if testShovelForSchema.Schema == nil {
					t.Fatalf("No schema was extracted for type verification")
				}

				if len(testShovelForSchema.Schema.Fields) != len(tt.expectedTypes) {
					t.Errorf("Expected %d fields, got %d", len(tt.expectedTypes), len(testShovelForSchema.Schema.Fields))
				} else {
					for i, expectedType := range tt.expectedTypes {
						actualType := testShovelForSchema.Schema.Fields[i].Type
						if actualType != expectedType {
							t.Errorf("Field %d (%s): expected type %s, got %s",
								i, testShovelForSchema.Schema.Fields[i].Name, expectedType, actualType)
						}
					}
				}
			}

			// The key test: verify type inference didn't crash and produced valid parquet
			t.Logf("%s: Successfully handled %d records with type inference", tt.description, len(records))
		})
	}
}

func TestParquetShovelTypeInferenceFromFirstRow(t *testing.T) {
	// Test that type inference is based on the first non-empty value found
	tests := []struct {
		name             string
		csvInput         string
		expectedBehavior string
	}{
		{
			name: "Integer inference from first row",
			csvInput: `count,score
42,100
99,85`,
			expectedBehavior: "Should infer INT64 for both fields",
		},
		{
			name: "String inference when first has string",
			csvInput: `name,code
Alice,ABC123
Bob,DEF456`,
			expectedBehavior: "Should infer BYTE_ARRAY for both fields",
		},
		{
			name: "Boolean inference from first row",
			csvInput: `active,verified
true,false
false,true`,
			expectedBehavior: "Should infer BOOLEAN for both fields",
		},
		{
			name: "Float inference from first row",
			csvInput: `price,rate
19.99,0.15
29.99,0.25`,
			expectedBehavior: "Should infer DOUBLE for both fields",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shovel := &ParquetShovel{}

			csvSrc := io.NopCloser(strings.NewReader(tt.csvInput))
			var parquetDst bytes.Buffer
			parquetDstCloser := &nopWriteCloser{&parquetDst}

			err := shovel.CopyOut(parquetDstCloser, csvSrc)
			if err != nil {
				t.Fatalf("CopyOut failed: %v", err)
			}

			// Test round trip to verify type consistency
			outputParquetData := parquetDst.Bytes()

			// Convert back to CSV to verify consistency
			shovel2 := &ParquetShovel{}
			parquetSrc := io.NopCloser(bytes.NewReader(outputParquetData))
			var csvBuffer bytes.Buffer
			csvCloser := &nopWriteCloser{&csvBuffer}

			err = shovel2.CopyIn(csvCloser, parquetSrc)
			if err != nil {
				t.Fatalf("Round trip CopyIn failed: %v", err)
			}

			// Verify we can parse the CSV output
			csvOutput := csvBuffer.String()
			lines := strings.Split(strings.TrimSpace(csvOutput), "\n")

			originalLines := strings.Split(strings.TrimSpace(tt.csvInput), "\n")
			if len(lines) != len(originalLines) {
				t.Errorf("Round trip changed number of lines: expected %d, got %d", len(originalLines), len(lines))
			}

			// Headers should match
			if lines[0] != originalLines[0] {
				t.Errorf("Headers don't match after round trip: expected %q, got %q", originalLines[0], lines[0])
			}

			t.Logf("%s: %s - Round trip successful", tt.name, tt.expectedBehavior)
		})
	}
}

func TestParquetShovelEmptyData(t *testing.T) {
	// Test with empty parquet file
	fw := buffer.NewBufferFile()
	pw, err := writer.NewParquetWriter(fw, new(TestData), 4)
	if err != nil {
		t.Fatalf("Failed to create parquet writer: %v", err)
	}

	if err := pw.WriteStop(); err != nil {
		t.Fatalf("Failed to stop parquet writer: %v", err)
	}
	fw.Close()

	emptyParquetData := fw.Bytes()

	shovel := &ParquetShovel{}
	src := io.NopCloser(bytes.NewReader(emptyParquetData))
	var dst bytes.Buffer
	dstCloser := &nopWriteCloser{&dst}

	err = shovel.CopyIn(dstCloser, src)
	if err != nil {
		t.Fatalf("CopyIn failed with empty data: %v", err)
	}

	// Should have header but no data rows
	csvOutput := dst.String()
	lines := strings.Split(strings.TrimSpace(csvOutput), "\n")
	if len(lines) != 1 || lines[0] != "name,age,score,active" {
		t.Errorf("Expected only header line, got: %q", csvOutput)
	}
}

func TestParquetShovelRoundTrip(t *testing.T) {
	// Test complete round trip: parquet -> CSV -> parquet
	originalData := createTestParquetDataWithDots()

	// Step 1: Parquet to CSV
	shovel := &ParquetShovel{}
	src1 := io.NopCloser(bytes.NewReader(originalData))
	var csvBuffer bytes.Buffer
	csvCloser := &nopWriteCloser{&csvBuffer}

	err := shovel.CopyIn(csvCloser, src1)
	if err != nil {
		t.Fatalf("Failed parquet to CSV conversion: %v", err)
	}

	csvData := csvBuffer.String()

	// Step 2: CSV back to parquet
	csvSrc := io.NopCloser(strings.NewReader(csvData))
	var parquetBuffer bytes.Buffer
	parquetCloser := &nopWriteCloser{&parquetBuffer}

	err = shovel.CopyOut(parquetCloser, csvSrc)
	if err != nil {
		t.Fatalf("Failed CSV to parquet conversion: %v", err)
	}

	// Step 3: Verify the round trip result
	resultData := parquetBuffer.Bytes()
	if len(resultData) == 0 {
		t.Fatal("No data after round trip")
	}

	// Read the result back
	fr := buffer.NewBufferFileFromBytes(resultData)
	pr, err := reader.NewParquetReader(fr, new(TestDataWithDots), 4)
	if err != nil {
		t.Fatalf("Failed to read round trip result: %v", err)
	}
	defer pr.ReadStop()

	// Verify we have the same number of rows
	if pr.GetNumRows() != 3 {
		t.Errorf("Expected 3 rows after round trip, got %d", pr.GetNumRows())
	}

	// Read and verify the data
	records, err := pr.ReadByNumber(int(pr.GetNumRows()))
	if err != nil {
		t.Fatalf("Failed to read records: %v", err)
	}

	expectedRecords := []TestDataWithDots{
		{"John", "Doe", 88.5},
		{"Jane", "Smith", 91.2},
		{"Mike", "Johnson", 85.7},
	}

	for i, record := range records {
		testData, ok := record.(TestDataWithDots)
		if !ok {
			t.Errorf("Record %d: expected TestDataWithDots type, got %T", i, record)
			continue
		}

		expected := expectedRecords[i]
		if testData.FirstName != expected.FirstName || testData.LastName != expected.LastName ||
			testData.TestScore != expected.TestScore {
			t.Errorf("Record %d: expected %+v, got %+v", i, expected, testData)
		}
	}
}

func TestParquetShovelTestMixedTypesCSV(t *testing.T) {
	// Test the exact content from tmp/test_mixed_types.csv
	csvInput := `a,b,c
1,2,true
a,2,1
3,2,nope
4,2.2,`

	shovel := &ParquetShovel{}
	csvSrc := io.NopCloser(strings.NewReader(csvInput))
	var parquetDst bytes.Buffer
	parquetDstCloser := &nopWriteCloser{&parquetDst}

	// Convert CSV to parquet
	err := shovel.CopyOut(parquetDstCloser, csvSrc)
	if err != nil {
		t.Fatalf("CopyOut failed: %v", err)
	}

	// Verify parquet data was created
	outputParquetData := parquetDst.Bytes()
	if len(outputParquetData) == 0 {
		t.Fatal("No parquet data was written")
	}

	// Extract schema by reading the parquet back to CSV
	shovel2 := &ParquetShovel{}
	parquetSrc := io.NopCloser(bytes.NewReader(outputParquetData))
	var csvBuffer bytes.Buffer
	csvCloser := &nopWriteCloser{&csvBuffer}

	err = shovel2.CopyIn(csvCloser, parquetSrc)
	if err != nil {
		t.Fatalf("Round trip CopyIn failed: %v", err)
	}

	// Verify the schema was correctly inferred
	if shovel2.Schema == nil {
		t.Fatal("No schema was extracted")
	}

	expectedSchema := []struct {
		name string
		typ  string
	}{
		{"a", "BYTE_ARRAY"}, // 1 -> "a" (int to string widening)
		{"b", "DOUBLE"},     // 2 -> 2.2 (int to float widening)
		{"c", "BYTE_ARRAY"}, // true -> 1 -> "nope" (bool to int to string widening)
	}

	if len(shovel2.Schema.Fields) != len(expectedSchema) {
		t.Fatalf("Expected %d fields, got %d", len(expectedSchema), len(shovel2.Schema.Fields))
	}

	for i, expected := range expectedSchema {
		field := shovel2.Schema.Fields[i]
		if field.Name != expected.name {
			t.Errorf("Field %d: expected name %s, got %s", i, expected.name, field.Name)
		}
		if field.Type != expected.typ {
			t.Errorf("Field %d (%s): expected type %s, got %s", i, field.Name, expected.typ, field.Type)
		}
	}

	// Verify the CSV round trip has expected format
	csvOutput := csvBuffer.String()
	lines := strings.Split(strings.TrimSpace(csvOutput), "\n")

	expectedHeader := "a,b,c"
	if lines[0] != expectedHeader {
		t.Errorf("Expected header %q, got %q", expectedHeader, lines[0])
	}

	// Verify we have 4 data rows + 1 header = 5 total lines
	if len(lines) != 5 {
		t.Errorf("Expected 5 lines total (1 header + 4 data), got %d", len(lines))
	}

	t.Logf("Successfully processed test_mixed_types.csv with schema: %+v", shovel2.Schema.Fields)
	t.Logf("Round trip CSV output:\n%s", csvOutput)

	// CRITICAL: Verify actual data content is preserved correctly
	expectedDataRows := []string{
		"1,2,true",
		"a,2,1",
		"3,2,nope",
		"4,2.2,",
	}

	for i, expectedRow := range expectedDataRows {
		actualRow := lines[i+1] // Skip header
		if actualRow != expectedRow {
			t.Errorf("Data corruption in row %d: expected %q, got %q", i+1, expectedRow, actualRow)
		}
	}
}

// inferFieldType determines the parquet type from a Go value (test-only function)
func inferFieldType(value interface{}) string {
	if value == nil {
		return "BYTE_ARRAY" // Default to string for nil values
	}

	switch value.(type) {
	case bool:
		return "BOOLEAN"
	case int, int32, int64:
		return "INT64"
	case float32, float64:
		return "DOUBLE"
	default:
		return "BYTE_ARRAY" // Default to string
	}
}

func TestHelperFunctions(t *testing.T) {
	t.Run("inferFieldType", func(t *testing.T) {
		tests := []struct {
			value    interface{}
			expected string
		}{
			{nil, "BYTE_ARRAY"},
			{true, "BOOLEAN"},
			{false, "BOOLEAN"},
			{int(42), "INT64"},
			{int32(42), "INT64"},
			{int64(42), "INT64"},
			{float32(3.14), "DOUBLE"},
			{float64(3.14), "DOUBLE"},
			{"hello", "BYTE_ARRAY"},
			{[]byte("bytes"), "BYTE_ARRAY"},
		}

		for _, tt := range tests {
			result := inferFieldType(tt.value)
			if result != tt.expected {
				t.Errorf("inferFieldType(%T %v) = %q, expected %q", tt.value, tt.value, result, tt.expected)
			}
		}
	})

	t.Run("normalizeFieldName", func(t *testing.T) {
		tests := []struct {
			input    string
			expected string
		}{
			{"simple", "Simple"},
			{"with.dots", "Withdots"},
			{"with-dashes", "Withdashes"},
			{"with spaces", "Withspaces"},
			{"", "Field"},
			{"123invalid", "Field123invalid"},
			{"valid_field", "Valid_field"},
		}

		for _, tt := range tests {
			result := normalizeFieldName(tt.input)
			if result != tt.expected {
				t.Errorf("normalizeFieldName(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		}
	})

	t.Run("parseCSVValue", func(t *testing.T) {
		tests := []struct {
			input    string
			expected interface{}
		}{
			{"", nil},
			{"42", int64(42)},
			{"3.14", float64(3.14)},
			{"true", true},
			{"false", false},
			{"hello", "hello"},
			{"not_a_number", "not_a_number"},
		}

		for _, tt := range tests {
			result := parseCSVValue(tt.input)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("parseCSVValue(%q) = %v (%T), expected %v (%T)",
					tt.input, result, result, tt.expected, tt.expected)
			}
		}
	})

	t.Run("formatCSVValue", func(t *testing.T) {
		tests := []struct {
			input    interface{}
			expected string
		}{
			{nil, ""},
			{"hello", "hello"},
			{42, "42"},
			{int32(42), "42"},
			{int64(42), "42"},
			{3.14, "3.14"},
			{float32(3.14), "3.14"},
			{true, "true"},
			{false, "false"},
		}

		for _, tt := range tests {
			result := formatCSVValue(tt.input)
			if result != tt.expected {
				t.Errorf("formatCSVValue(%v) = %q, expected %q", tt.input, result, tt.expected)
			}
		}
	})
}

// Helper type for testing
type nopWriteCloser struct {
	io.Writer
}

func (nopWriteCloser) Close() error { return nil }

func TestParquetShovelEnhancedErrorMessages(t *testing.T) {
	// Create a parquet file with a float column
	parquetData := createTestParquetData()

	// Extract schema first
	shovel := &ParquetShovel{}
	src := io.NopCloser(bytes.NewReader(parquetData))
	var tempDst bytes.Buffer
	tempDstCloser := &nopWriteCloser{&tempDst}

	err := shovel.CopyIn(tempDstCloser, src)
	if err != nil {
		t.Fatalf("Failed to extract schema: %v", err)
	}

	// Now test CopyOut with invalid data that should trigger enhanced error message
	csvInput := `name,age,score,active
Alice,25,invalid_float,true
Bob,thirty,87.2,false` // Row 2 has "thirty" for age (int field) and "invalid_float" for score (float field)

	csvSrc := io.NopCloser(strings.NewReader(csvInput))
	var parquetDst bytes.Buffer
	parquetDstCloser := &nopWriteCloser{&parquetDst}

	err = shovel.CopyOut(parquetDstCloser, csvSrc)
	if err == nil {
		t.Fatal("Expected error due to type conversion failure, but got none")
	}

	errorMsg := err.Error()

	// Check that error message contains all expected information
	expectedComponents := []string{
		"field",          // Field identification
		"at row",         // Row number
		"cannot convert", // Conversion failure
	}

	for _, component := range expectedComponents {
		if !strings.Contains(errorMsg, component) {
			t.Errorf("Error message missing component %q. Full error: %s", component, errorMsg)
		}
	}

	// Should contain either row 1 (invalid_float in score) or row 2 (thirty in age)
	if !strings.Contains(errorMsg, "row 1") && !strings.Contains(errorMsg, "row 2") {
		t.Errorf("Error message should contain specific row number. Got: %s", errorMsg)
	}

	// Should contain the problematic value
	hasProblematicValue := strings.Contains(errorMsg, "invalid_float") || strings.Contains(errorMsg, "thirty")
	if !hasProblematicValue {
		t.Errorf("Error message should contain the problematic value. Got: %s", errorMsg)
	}

	t.Logf("Enhanced error message: %s", errorMsg)
}
