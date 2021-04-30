package ledgerbackend

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"

	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"

	"github.com/pelletier/go-toml"
)

const (
	defaultHTTPPort      = 11626
	defaultFailureSafety = -1

	// if LOG_FILE_PATH is omitted stellar core actually defaults to "stellar-core.log"
	// however, we are overriding this default for captive core
	defaultLogFilePath = "" // by default we disable logging to a file

	// if DISABLE_XDR_FSYNC is omitted stellar core actually defaults to false
	// however, we are overriding this default for captive core
	defaultDisableXDRFsync = true
)

var validQuality = map[string]bool{
	"CRITICAL": true,
	"HIGH":     true,
	"MEDIUM":   true,
	"LOW":      true,
}

type Validator struct {
	Name       string `toml:"NAME"`
	Quality    string `toml:"QUALITY,omitempty"`
	HomeDomain string `toml:"HOME_DOMAIN"`
	PublicKey  string `toml:"PUBLIC_KEY"`
	Address    string `toml:"ADDRESS,omitempty"`
	History    string `toml:"HISTORY,omitempty"`
}

type HomeDomain struct {
	HomeDomain string `toml:"HOME_DOMAIN"`
	Quality    string `toml:"QUALITY"`
}

type History struct {
	Get string `toml:"get"`
	// should we allow put and mkdir for captive core?
	Put   string `toml:"put,omitempty"`
	Mkdir string `toml:"mkdir,omitempty"`
}

type QuorumSet struct {
	ThresholdPercent int      `toml:"THRESHOLD_PERCENT"`
	Validators       []string `toml:"VALIDATORS"`
}

type captiveCoreTomlValues struct {
	// we cannot omitempty because the empty string is a valid configuration for LOG_FILE_PATH
	// and the default is stellar-core.log
	LogFilePath   string `toml:"LOG_FILE_PATH"`
	BucketDirPath string `toml:"BUCKET_DIR_PATH,omitempty"`
	// we cannot omitempty because 0 is a valid configuration for HTTP_PORT
	// and the default is 11626
	HTTPPort          uint     `toml:"HTTP_PORT"`
	PublicHTTPPort    bool     `toml:"PUBLIC_HTTP_PORT,omitempty"`
	NodeNames         []string `toml:"NODE_NAMES,omitempty"`
	NetworkPassphrase string   `toml:"NETWORK_PASSPHRASE,omitempty"`
	PeerPort          uint     `toml:"PEER_PORT,omitempty"`
	// we cannot omitempty because 0 is a valid configuration for FAILURE_SAFETY
	// and the default is -1
	FailureSafety                        int                  `toml:"FAILURE_SAFETY"`
	UnsafeQuorum                         bool                 `toml:"UNSAFE_QUORUM,omitempty"`
	RunStandalone                        bool                 `toml:"RUN_STANDALONE,omitempty"`
	ArtificiallyAccelerateTimeForTesting bool                 `toml:"ARTIFICIALLY_ACCELERATE_TIME_FOR_TESTING,omitempty"`
	DisableXDRFsync                      bool                 `toml:"DISABLE_XDR_FSYNC,omitempty"`
	HomeDomains                          []HomeDomain         `toml:"HOME_DOMAINS,omitempty"`
	Validators                           []Validator          `toml:"VALIDATORS,omitempty"`
	HistoryEntries                       map[string]History   `toml:"-"`
	QuorumSetEntries                     map[string]QuorumSet `toml:"-"`
}

func (c *captiveCoreTomlValues) QuorumSetIsConfigured() bool {
	return len(c.QuorumSetEntries) > 0 || len(c.Validators) > 0
}

func (c *captiveCoreTomlValues) historyIsConfigured() bool {
	if len(c.HistoryEntries) > 0 {
		return true
	}
	for _, v := range c.Validators {
		if v.History != "" {
			return true
		}
	}
	return false
}

type CaptiveCoreToml struct {
	captiveCoreTomlValues
	tree      *toml.Tree
	separator string
}

// maxConsecutiveChar returns the length of the longest substring of repeating `char` values found in `s`.
func maxConsecutiveChar(s string, char byte) int {
	var count, max int
	for i := range s {
		if s[i] == char {
			count++
		} else {
			if count > max {
				max = count
			}
			count = 0
		}
	}
	return max
}

// nonSubstring returns a string which is guaranteed to not be a substring of `text`.
func nonSubstring(text string) string {
	return strings.Repeat("-", maxConsecutiveChar(text, '-')+1)
}

// flattenTables will transform a given toml text by flattening all nested tables
// whose root can be found in `rootNames`.
//
// In the TOML spec dotted keys represents nesting. So we flatten the table key by replacing instances of "."
// with `separator`. For example:
//
// text := `[QUORUM_SET.a.b.c]
//         THRESHOLD_PERCENT=67
//         VALIDATORS=["a","b"]`
// flattenTables(text, []string{"QUORUM_SET"}, "-") ->
//
// `[QUORUM_SET-a-b-c]
// THRESHOLD_PERCENT=67
// VALIDATORS=["a","b"]`
func flattenTables(text string, rootNames []string, separator string) string {
	orExpression := strings.Join(rootNames, "|")
	re := regexp.MustCompile("\\[(" + orExpression + ")(\\..+)?\\]")

	return re.ReplaceAllStringFunc(text, func(match string) string {
		return strings.ReplaceAll(match, ".", separator)
	})
}

func (c *CaptiveCoreToml) Marshall() ([]byte, error) {
	var sb strings.Builder
	sb.WriteString("# Generated file, do not edit\n")
	encoder := toml.NewEncoder(&sb)
	if err := encoder.Encode(c.captiveCoreTomlValues); err != nil {
		return nil, errors.Wrap(err, "could not encode toml file")
	}

	if len(c.HistoryEntries) > 0 {
		if err := encoder.Encode(c.HistoryEntries); err != nil {
			return nil, errors.Wrap(err, "could not encode history entries")
		}
	}

	if len(c.QuorumSetEntries) > 0 {
		if err := encoder.Encode(c.QuorumSetEntries); err != nil {
			return nil, errors.Wrap(err, "could not encode quorum set")
		}
	}

	unflattenedToml := strings.ReplaceAll(sb.String(), c.separator, ".")
	return []byte(unflattenedToml), nil
}

func unmarshalTreeNode(t *toml.Tree, key string, dest interface{}) error {
	tree, ok := t.Get(key).(*toml.Tree)
	if !ok {
		return fmt.Errorf("unexpected key %v", key)
	}
	return tree.Unmarshal(dest)
}

func (c *CaptiveCoreToml) unmarshal(text string) error {
	var body captiveCoreTomlValues
	quorumSetEntries := map[string]QuorumSet{}
	historyEntries := map[string]History{}
	separator := nonSubstring(text)
	// The toml library has trouble with nested tables so we need to flatten all nested
	// QUORUM_SET and HISTORY tables as a workaround.
	// In Marshall() we apply the inverse process to unflatten the nested tables.
	text = flattenTables(text, []string{"QUORUM_SET", "HISTORY"}, separator)

	data := []byte(text)
	tree, err := toml.Load(text)
	if err != nil {
		return err
	}

	err = toml.NewDecoder(bytes.NewReader(data)).Decode(&body)
	if err != nil {
		return err
	}

	for _, key := range tree.Keys() {
		switch {
		case strings.HasPrefix(key, "QUORUM_SET"):
			var qs QuorumSet
			if err = unmarshalTreeNode(tree, key, &qs); err != nil {
				return err
			}
			quorumSetEntries[key] = qs
		case strings.HasPrefix(key, "HISTORY"):
			var h History
			if err = unmarshalTreeNode(tree, key, &h); err != nil {
				return err
			}
			historyEntries[key] = h
		}
	}

	c.tree = tree
	c.captiveCoreTomlValues = body
	c.separator = separator
	c.QuorumSetEntries = quorumSetEntries
	c.HistoryEntries = historyEntries
	return nil
}

type CaptiveCoreTomlParams struct {
	// NetworkPassphrase is the Stellar network passphrase used by captive core when connecting to the Stellar network
	NetworkPassphrase string
	// HistoryArchiveURLs are a list of history archive urls
	HistoryArchiveURLs []string
	// HTTPPort is the TCP port to listen for requests (defaults to 0, which disables the HTTP server)
	HTTPPort *uint
	// PeerPort is the TCP port to bind to for connecting to the Stellar network
	// (defaults to 11625). It may be useful for example when there's >1 Stellar-Core
	// instance running on a machine.
	PeerPort *uint
	// LogPath is the (optional) path in which to store Core logs, passed along
	// to Stellar Core's LOG_FILE_PATH
	LogPath *string
}

func NewCaptiveCoreTomlFromFile(configPath string, params CaptiveCoreTomlParams) (*CaptiveCoreToml, error) {
	var captiveCoreToml CaptiveCoreToml
	text, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, errors.Wrap(err, "could not load toml path")
	}

	if err = captiveCoreToml.unmarshal(string(text)); err != nil {
		return nil, errors.Wrap(err, "could not unmarshall captive core toml")
	}

	if err = captiveCoreToml.validate(params); err != nil {
		return nil, errors.Wrap(err, "invalid captive core toml")
	}

	captiveCoreToml.setDefaults(params)
	return &captiveCoreToml, nil
}

func NewCaptiveCoreToml(params CaptiveCoreTomlParams) (*CaptiveCoreToml, error) {
	var captiveCoreToml CaptiveCoreToml
	var err error
	captiveCoreToml.tree, err = toml.TreeFromMap(map[string]interface{}{})
	captiveCoreToml.separator = "---"
	captiveCoreToml.setDefaults(params)
	return &captiveCoreToml, err
}

func (c *CaptiveCoreToml) CatchupToml() *CaptiveCoreToml {
	offline := *c
	offline.RunStandalone = true
	offline.UnsafeQuorum = true
	offline.PublicHTTPPort = false
	offline.HTTPPort = 0
	offline.FailureSafety = 0

	if !c.QuorumSetIsConfigured() {
		// Add a fictional quorum -- necessary to convince core to start up;
		// but not used at all for our purposes. Pubkey here is just random.
		offline.QuorumSetEntries = map[string]QuorumSet{
			"QUORUM_SET": QuorumSet{
				ThresholdPercent: 100,
				Validators:       []string{"GCZBOIAY4HLKAJVNJORXZOZRAY2BJDBZHKPBHZCRAIUR5IHC2UHBGCQR"},
			},
		}
	}
	return &offline
}

func (c *CaptiveCoreToml) setDefaults(params CaptiveCoreTomlParams) {
	if !c.tree.Has("NETWORK_PASSPHRASE") {
		c.NetworkPassphrase = params.NetworkPassphrase
	}

	if def := c.tree.Has("HTTP_PORT"); !def && params.HTTPPort != nil {
		c.HTTPPort = *params.HTTPPort
	} else if !def && params.HTTPPort == nil {
		c.HTTPPort = defaultHTTPPort
	}

	if def := c.tree.Has("PEER_PORT"); !def && params.PeerPort != nil {
		c.PeerPort = *params.PeerPort
	}

	if def := c.tree.Has("LOG_FILE_PATH"); !def && params.LogPath != nil {
		c.LogFilePath = *params.LogPath
	} else if !def && params.LogPath == nil {
		c.LogFilePath = defaultLogFilePath
	}

	if !c.tree.Has("FAILURE_SAFETY") {
		c.FailureSafety = defaultFailureSafety
	}
	if !c.tree.Has("DISABLE_XDR_FSYNC") {
		c.DisableXDRFsync = defaultDisableXDRFsync
	}

	if !c.historyIsConfigured() {
		c.HistoryEntries = map[string]History{}
		for i, val := range params.HistoryArchiveURLs {
			c.HistoryEntries[fmt.Sprintf("HISTORY%sh%d", c.separator, i)] = History{
				Get: fmt.Sprintf("curl -sf %s/{0} -o {1}", val),
			}
		}
	}
}

func (c *CaptiveCoreToml) validate(params CaptiveCoreTomlParams) error {
	if def := c.tree.Has("NETWORK_PASSPHRASE"); def && c.NetworkPassphrase != params.NetworkPassphrase {
		return fmt.Errorf(
			"NETWORK_PASSPHRASE in captive core config file: %s does not match Horizon network-passphrase flag: %s",
			c.NetworkPassphrase,
			params.NetworkPassphrase,
		)
	}

	if def := c.tree.Has("HTTP_PORT"); def && params.HTTPPort != nil && c.HTTPPort != *params.HTTPPort {
		return fmt.Errorf(
			"HTTP_PORT in captive core config file: %d does not match Horizon captive-core-http-port flag: %d",
			c.HTTPPort,
			*params.HTTPPort,
		)
	}

	if def := c.tree.Has("PEER_PORT"); def && params.PeerPort != nil && c.PeerPort != *params.PeerPort {
		return fmt.Errorf(
			"PEER_PORT in captive core config file: %d does not match Horizon captive-core-peer-port flag: %d",
			c.PeerPort,
			*params.PeerPort,
		)
	}

	if def := c.tree.Has("LOG_FILE_PATH"); def && params.LogPath != nil && c.LogFilePath != *params.LogPath {
		return fmt.Errorf(
			"LOG_FILE_PATH in captive core config file: %s does not match Horizon captive-core-log-path flag: %s",
			c.LogFilePath,
			*params.LogPath,
		)
	}

	homeDomainSet := map[string]HomeDomain{}
	for _, hd := range c.HomeDomains {
		if _, ok := homeDomainSet[hd.HomeDomain]; ok {
			return fmt.Errorf(
				"found duplicate home domain in captive core configuration: %s",
				hd.HomeDomain,
			)
		}
		if hd.HomeDomain == "" {
			return fmt.Errorf(
				"found invalid home domain entry which is missing a HOME_DOMAIN value: %s",
				hd.HomeDomain,
			)
		}
		if hd.Quality == "" {
			return fmt.Errorf(
				"found invalid home domain entry which is missing a QUALITY value: %s",
				hd.HomeDomain,
			)
		}
		if !validQuality[hd.Quality] {
			return fmt.Errorf(
				"found invalid home domain entry which has an invalid QUALITY value: %s",
				hd.HomeDomain,
			)
		}
		homeDomainSet[hd.HomeDomain] = hd
	}

	names := map[string]bool{}
	for _, v := range c.Validators {
		if names[v.Name] {
			return fmt.Errorf(
				"found duplicate validator in captive core configuration: %s",
				v.Name,
			)
		}
		if v.Name == "" {
			return fmt.Errorf(
				"found invalid validator entry which is missing a NAME value: %s",
				v.Name,
			)
		}
		if v.HomeDomain == "" {
			return fmt.Errorf(
				"found invalid validator entry which is missing a HOME_DOMAIN value: %s",
				v.Name,
			)
		}
		if v.PublicKey == "" {
			return fmt.Errorf(
				"found invalid validator entry which is missing a PUBLIC_KEY value: %s",
				v.Name,
			)
		}
		if _, err := xdr.AddressToAccountId(v.PublicKey); err != nil {
			return fmt.Errorf(
				"found invalid validator entry which has an invalid PUBLIC_KEY : %s",
				v.Name,
			)
		}
		if v.Quality == "" {
			if _, ok := homeDomainSet[v.HomeDomain]; !ok {
				return fmt.Errorf(
					"found invalid validator entry which is missing a QUALITY value: %s",
					v.Name,
				)
			}
		} else if !validQuality[v.Quality] {
			return fmt.Errorf(
				"found invalid validator entry which has an invalid QUALITY value: %s",
				v.Name,
			)
		}

		names[v.Name] = true
	}

	return nil
}
