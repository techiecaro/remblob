#!/bin/bash

# Get version information
VERSION=$(git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
BUILT_BY=${USER:-$(whoami)}

# Build with version information
go build -ldflags "
    -X techiecaro/remblob/version.Version=$VERSION
    -X techiecaro/remblob/version.Commit=$COMMIT
    -X techiecaro/remblob/version.Date=$DATE
    -X techiecaro/remblob/version.BuiltBy=$BUILT_BY
" "$@"

echo "Built remblob $VERSION ($COMMIT) on $DATE by $BUILT_BY"
