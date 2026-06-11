package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

// generatedFileMode is the permission for files xdrgen writes. They are generated
// source, meant to be world-readable like the rest of the checked-in tree.
const generatedFileMode = 0o644

func main() {
	inputPath := flag.String("input", "", "Path to JSON IR file from Rust XDR parser")
	outputDir := flag.String("output", "", "Output directory for generated Go files")
	flag.Parse()

	if *inputPath == "" || *outputDir == "" {
		fmt.Fprintf(os.Stderr, "Usage: xdrgen -input <ir.json> -output <dir>\n")
		os.Exit(1)
	}

	// Read and parse the IR
	data, err := os.ReadFile(*inputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading IR: %v\n", err)
		os.Exit(1)
	}

	var ir IR
	if err = json.Unmarshal(data, &ir); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing IR: %v\n", err)
		os.Exit(1)
	}

	// Build lookup tables
	gen, err := NewGenerator(&ir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error building generator: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "Loaded IR: %d definitions\n", len(gen.allDefs))

	// Generate views file
	viewsContent, err := gen.GenerateViews()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating views: %v\n", err)
		os.Exit(1)
	}

	viewsPath := filepath.Join(*outputDir, "xdr_views_generated.go")
	if err := os.WriteFile(viewsPath, viewsContent, generatedFileMode); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing views: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "Generated: %s (%d bytes)\n", viewsPath, len(viewsContent))

	// Generate the conformance-harness registry (typedValueViewTypes) alongside
	// the views, derived from the views source so it stays in lockstep with them.
	registryContent, err := GenerateViewsRegistry(viewsContent)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating views registry: %v\n", err)
		os.Exit(1)
	}

	registryPath := filepath.Join(*outputDir, "xdr_views_registry_generated_test.go")
	if err := os.WriteFile(registryPath, registryContent, generatedFileMode); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing views registry: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "Generated: %s (%d bytes)\n", registryPath, len(registryContent))
}
