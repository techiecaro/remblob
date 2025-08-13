package version

import (
	"fmt"
	"runtime"
)

// Build-time variables (injected by goreleaser or build scripts)
var (
	// Version is the semantic version of the build
	Version = "dev"
	// Commit is the git commit hash
	Commit = "unknown"
	// Date is the build date
	Date = "unknown"
	// BuiltBy indicates who/what built the binary
	BuiltBy = "unknown"
)

// Info contains version information
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	Date      string `json:"date"`
	BuiltBy   string `json:"built_by"`
	GoVersion string `json:"go_version"`
	Platform  string `json:"platform"`
}

// Get returns the current version information
func Get() Info {
	return Info{
		Version:   Version,
		Commit:    Commit,
		Date:      Date,
		BuiltBy:   BuiltBy,
		GoVersion: runtime.Version(),
		Platform:  fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}

// String returns a human-readable version string
func (i Info) String() string {
	result := fmt.Sprintf("remblob version %s", i.Version)

	if i.Commit != "unknown" {
		commitHash := i.Commit
		if len(commitHash) > 8 {
			commitHash = commitHash[:8]
		}
		result += fmt.Sprintf(" (%s)", commitHash)
	}

	if i.Date != "unknown" {
		result += fmt.Sprintf(" built on %s", i.Date)
	}

	if i.BuiltBy != "unknown" {
		result += fmt.Sprintf(" by %s", i.BuiltBy)
	}

	result += fmt.Sprintf(" (%s, %s)", i.GoVersion, i.Platform)

	return result
}

// Short returns a short version string
func (i Info) Short() string {
	return i.Version
}
