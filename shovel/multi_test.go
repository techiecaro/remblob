package shovel

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// TestMultiShovelParquetStatePersistence tests that parquet shovel state is preserved between CopyIn and CopyOut
func TestMultiShovelParquetStatePersistence(t *testing.T) {
	// Create a MultiShovel configured for parquet to parquet
	multiShovel := &MultiShovel{
		SourceParquet:      true,
		DestinationParquet: true,
	}

	// Create some parquet test data first (using the existing test helper)
	parquetData := createTestParquetData()

	// Step 1: Use CopyIn to read parquet file (this should create and store the ParquetShovel)
	parquetSrc := io.NopCloser(bytes.NewReader(parquetData))
	var csvBuffer bytes.Buffer
	csvDst := &nopWriteCloser{&csvBuffer}

	err := multiShovel.CopyIn(csvDst, parquetSrc)
	if err != nil {
		t.Fatalf("CopyIn failed: %v", err)
	}

	// Verify that a shovel instance was created and stored
	if multiShovel.shovelInstance == nil {
		t.Fatal("No shovel instance was stored after CopyIn")
	}

	// Check that it's a ParquetShovel
	parquetShovel, ok := multiShovel.shovelInstance.(*ParquetShovel)
	if !ok {
		t.Fatalf("Expected ParquetShovel, got %T", multiShovel.shovelInstance)
	}

	// Verify that the schema was captured during CopyIn
	if parquetShovel.Schema == nil {
		t.Fatal("ParquetShovel schema was not captured during CopyIn")
	}

	if len(parquetShovel.Schema.Fields) != 4 {
		t.Errorf("Expected 4 schema fields, got %d", len(parquetShovel.Schema.Fields))
	}

	// Step 2: Now use CopyOut to write back to parquet - this should reuse the same shovel instance
	csvInput := csvBuffer.String()
	csvSrc := io.NopCloser(strings.NewReader(csvInput))
	var parquetBuffer bytes.Buffer
	parquetDst := &nopWriteCloser{&parquetBuffer}

	err = multiShovel.CopyOut(parquetDst, csvSrc)
	if err != nil {
		t.Fatalf("CopyOut failed: %v", err)
	}

	// Verify the parquet data was created
	parquetOutput := parquetBuffer.Bytes()
	if len(parquetOutput) == 0 {
		t.Fatal("No parquet data was written")
	}

	t.Logf("MultiShovel successfully preserved parquet state between operations")
	t.Logf("Schema fields: %+v", parquetShovel.Schema.Fields)
}

// TestMultiShovelDifferentTypes tests creating different shovels for different types
func TestMultiShovelDifferentTypes(t *testing.T) {
	// Create a MultiShovel configured for parquet source but plain destination
	multiShovel := &MultiShovel{
		SourceParquet:      true,
		DestinationParquet: false,
	}

	// Create parquet test data and read it with CopyIn (creates ParquetShovel)
	parquetData := createTestParquetData()
	parquetSrc := io.NopCloser(bytes.NewReader(parquetData))
	var csvBuffer bytes.Buffer
	csvDst := &nopWriteCloser{&csvBuffer}

	err := multiShovel.CopyIn(csvDst, parquetSrc)
	if err != nil {
		t.Fatalf("CopyIn failed: %v", err)
	}

	// Verify ParquetShovel was created and stored
	if multiShovel.shovelInstance == nil {
		t.Fatal("No shovel instance was created")
	}

	parquetShovel, ok := multiShovel.shovelInstance.(*ParquetShovel)
	if !ok {
		t.Fatalf("Expected ParquetShovel for source, got %T", multiShovel.shovelInstance)
	}

	// Now CopyOut to plain destination - should create a different shovel since types don't match
	csvInput := csvBuffer.String()
	csvSrc := io.NopCloser(strings.NewReader(csvInput))
	var plainBuffer bytes.Buffer
	plainDst := &nopWriteCloser{&plainBuffer}

	err = multiShovel.CopyOut(plainDst, csvSrc)
	if err != nil {
		t.Fatalf("CopyOut failed: %v", err)
	}

	// Verify the output is plain text (no parquet binary data)
	plainOutput := plainBuffer.String()
	if len(plainOutput) == 0 {
		t.Fatal("No output was written")
	}

	// Should contain CSV data since destination is plain
	if !strings.Contains(plainOutput, "name,age,score,active") {
		t.Error("Plain output should contain CSV header")
	}

	t.Logf("MultiShovel correctly handles different source/destination types")
	t.Logf("ParquetShovel schema preserved: %+v", parquetShovel.Schema.Fields)
}
