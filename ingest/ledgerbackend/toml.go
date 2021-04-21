package ledgerbackend

import (
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"

	"github.com/BurntSushi/toml"
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
	// we cannot omitzero because 0 is a valid configuration for HTTP_PORT
	// and the default is 11626
	HTTPPort          uint     `toml:"HTTP_PORT"`
	PublicHTTPPort    bool     `toml:"PUBLIC_HTTP_PORT,omitempty"`
	NodeNames         []string `toml:"NODE_NAMES,omitempty"`
	NetworkPassphrase string   `toml:"NETWORK_PASSPHRASE,omitempty"`
	PeerPort          uint     `toml:"PEER_PORT,omitzero"`
	// we cannot omitzero because 0 is a valid configuration for FAILURE_SAFETY
	// and the default is -1
	FailureSafety                        int                  `toml:"FAILURE_SAFETY"`
	UnsafeQuorum                         bool                 `toml:"UNSAFE_QUORUM,omitempty"`
	RunStandalone                        bool                 `toml:"RUN_STANDALONE,omitempty"`
	ArtificiallyAccelerateTimeForTesting bool                 `toml:"ARTIFICIALLY_ACCELERATE_TIME_FOR_TESTING,omitempty"`
	DisableXDRFsync                      bool                 `toml:"DISABLE_XDR_FSYNC,omitempty"`
	HomeDomains                          []HomeDomain         `toml:"HOME_DOMAINS,omitempty"`
	Validators                           []Validator          `toml:"VALIDATORS,omitempty"`
	HistoryEntries                       map[string]History   `toml:"HISTORY,omitempty"`
	QuorumSetEntries                     map[string]QuorumSet `toml:"QUORUM_SET,omitempty"`
}

func (c captiveCoreTomlValues) Marshall() ([]byte, error) {
	var sb strings.Builder
	sb.WriteString("# Generated file -- do not edit\n")
	if err := toml.NewEncoder(&sb).Encode(c); err != nil {
		return nil, errors.Wrap(err, "could not encode toml file")
	}
	return []byte(sb.String()), nil
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
	metadata toml.MetaData
}

func (c *CaptiveCoreToml) Unmarshal(text []byte) error {
	metadata, err := toml.Decode(string(text), &c.captiveCoreTomlValues)
	if err != nil {
		return err
	}
	c.metadata = metadata
	return nil
}

type CaptiveCoreTomlParams struct {
	ConfigPath string
	Strict     bool
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

func NewCaptiveCoreToml(params CaptiveCoreTomlParams) (*CaptiveCoreToml, error) {
	var captiveCoreToml CaptiveCoreToml
	if params.ConfigPath != "" {
		text, err := ioutil.ReadFile(params.ConfigPath)
		if err != nil {
			return nil, errors.Wrap(err, "could not load toml path")
		}
		if err = captiveCoreToml.Unmarshal(text); err != nil {
			return nil, errors.Wrap(err, "could not unmarshall captive core toml")
		}
		if err = captiveCoreToml.validate(params); err != nil {
			return nil, errors.Wrap(err, "invalid captive core toml")
		}
	}
	captiveCoreToml.setDefaults(params)

	return &captiveCoreToml, nil
}

func (c *CaptiveCoreToml) CatchupToml() *CaptiveCoreToml {
	offline := *c
	if !c.metadata.IsDefined("RUN_STANDALONE") {
		offline.RunStandalone = true
	}
	if !c.metadata.IsDefined("UNSAFE_QUORUM") {
		offline.UnsafeQuorum = true
	}
	if !c.QuorumSetIsConfigured() {
		// Add a fictional quorum -- necessary to convince core to start up;
		// but not used at all for our purposes. Pubkey here is just random.
		offline.QuorumSetEntries = map[string]QuorumSet{
			"generated": QuorumSet{
				ThresholdPercent: 100,
				Validators:       []string{"GCZBOIAY4HLKAJVNJORXZOZRAY2BJDBZHKPBHZCRAIUR5IHC2UHBGCQR"},
			},
		}
	}
	offline.PublicHTTPPort = false
	offline.HTTPPort = 0
	offline.FailureSafety = 0
	return &offline
}

func (c *CaptiveCoreToml) setDefaults(params CaptiveCoreTomlParams) {
	if !c.metadata.IsDefined("NETWORK_PASSPHRASE") {
		c.NetworkPassphrase = params.NetworkPassphrase
	}

	if def := c.metadata.IsDefined("HTTP_PORT"); !def && params.HTTPPort != nil {
		c.HTTPPort = *params.HTTPPort
	} else if !def && params.HTTPPort == nil {
		c.HTTPPort = defaultHTTPPort
	}

	if def := c.metadata.IsDefined("PEER_PORT"); !def && params.PeerPort != nil {
		c.PeerPort = *params.PeerPort
	}

	if def := c.metadata.IsDefined("LOG_FILE_PATH"); !def && params.LogPath != nil {
		c.LogFilePath = *params.LogPath
	} else if !def && params.LogPath == nil {
		c.LogFilePath = defaultLogFilePath
	}

	if !c.metadata.IsDefined("FAILURE_SAFETY") {
		c.FailureSafety = defaultFailureSafety
	}
	if !c.metadata.IsDefined("DISABLE_XDR_FSYNC") {
		c.DisableXDRFsync = defaultDisableXDRFsync
	}

	if !c.historyIsConfigured() {
		c.HistoryEntries = map[string]History{}
		for i, val := range params.HistoryArchiveURLs {
			c.HistoryEntries[fmt.Sprintf("h%d", i)] = History{
				Get: fmt.Sprintf("curl -sf %s/{0} -o {1}", val),
			}
		}
	}
}

func (c *CaptiveCoreToml) validate(params CaptiveCoreTomlParams) error {
	if params.Strict {
		if unexpected := c.metadata.Undecoded(); len(unexpected) > 0 {
			var parts []string
			for _, key := range unexpected {
				parts = append(parts, key.String())
			}
			return fmt.Errorf("invalid keys in captive core configuration: %s", strings.Join(parts, ", "))
		}
	}

	if def := c.metadata.IsDefined("NETWORK_PASSPHRASE"); def && c.NetworkPassphrase != params.NetworkPassphrase {
		return fmt.Errorf(
			"NETWORK_PASSPHRASE in captive core config file: %s does not match Horizon network-passphrase flag: %s",
			c.NetworkPassphrase,
			params.NetworkPassphrase,
		)
	}

	if def := c.metadata.IsDefined("HTTP_PORT"); def && params.HTTPPort != nil && c.HTTPPort != *params.HTTPPort {
		return fmt.Errorf(
			"HTTP_PORT in captive core config file: %d does not match Horizon captive-core-http-port flag: %d",
			c.HTTPPort,
			*params.HTTPPort,
		)
	}

	if def := c.metadata.IsDefined("PEER_PORT"); def && params.PeerPort != nil && c.PeerPort != *params.PeerPort {
		return fmt.Errorf(
			"PEER_PORT in captive core config file: %d does not match Horizon captive-core-peer-port flag: %d",
			c.PeerPort,
			*params.PeerPort,
		)
	}

	if def := c.metadata.IsDefined("LOG_FILE_PATH"); def && params.LogPath != nil && c.LogFilePath != *params.LogPath {
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
