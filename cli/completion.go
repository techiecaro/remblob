package cli

import (
	"fmt"
	"net/url"
	"os"
	"techiecaro/remblob/storage"

	"github.com/alecthomas/kong"
	"github.com/posener/complete"
	"github.com/willabides/kongplete"
)

func NewPathPredictor() complete.Predictor {
	return PathPredictor(storage.GetFileListerPrefixes())
}

type PathPredictor []string

func (p PathPredictor) Predict(args complete.Args) []string {

	pathPredictions := p.matchFileLister(args.Last)

	return append(p, pathPredictions...)
}

func (p PathPredictor) matchFileLister(pattern string) []string {
	if pattern == "" {
		return []string{}
	}

	prefixURL, err := url.Parse(pattern)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Can't parse %s\n", prefixURL)
		return []string{}
	}

	lister := storage.GetFileLister(*prefixURL)
	matchesURL := lister(*prefixURL)
	matches := make([]string, len(matchesURL))
	for i, match := range matchesURL {
		matches[i] = match.String()
	}

	return matches
}

// AddCompletion adds cli completion to an exising Kong parer
func AddCompletion(parser *kong.Kong) {
	// Run kongplete.Complete to handle completion requests
	kongplete.Complete(
		parser,
		kongplete.WithPredictor("path", NewPathPredictor()),
	)
}
