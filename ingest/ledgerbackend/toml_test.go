package ledgerbackend

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"path/filepath"
	"testing"
)

func newUint(v uint) *uint {
	return &v
}

func newString(s string) *string {
	return &s
}

func TestGenerateConfig(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		appendPath   string
		mode         stellarCoreRunnerMode
		expectedPath string
		httpPort     *uint
		peerPort     *uint
		logPath      *string
	}{
		{
			name:         "offline config with no appendix",
			mode:         stellarCoreRunnerModeOffline,
			appendPath:   "",
			expectedPath: filepath.Join("testdata", "expected-offline-core.cfg"),
			httpPort:     newUint(6789),
			peerPort:     newUint(12345),
			logPath:      nil,
		},
		{
			name:         "offline config with no peer port",
			mode:         stellarCoreRunnerModeOffline,
			appendPath:   "",
			expectedPath: filepath.Join("testdata", "expected-offline-with-no-peer-port.cfg"),
			httpPort:     newUint(6789),
			peerPort:     nil,
			logPath:      newString("/var/stellar-core/test.log"),
		},
		{
			name:         "online config with appendix",
			mode:         stellarCoreRunnerModeOnline,
			appendPath:   filepath.Join("testdata", "sample-appendix.cfg"),
			expectedPath: filepath.Join("testdata", "expected-online-core.cfg"),
			httpPort:     newUint(6789),
			peerPort:     newUint(12345),
			logPath:      nil,
		},
		{
			name:         "online config with no peer port",
			mode:         stellarCoreRunnerModeOnline,
			appendPath:   filepath.Join("testdata", "sample-appendix.cfg"),
			expectedPath: filepath.Join("testdata", "expected-online-with-no-peer-port.cfg"),
			httpPort:     newUint(6789),
			peerPort:     nil,
			logPath:      newString("/var/stellar-core/test.log"),
		},
		{
			name:         "online config with no http port",
			mode:         stellarCoreRunnerModeOnline,
			appendPath:   filepath.Join("testdata", "sample-appendix.cfg"),
			expectedPath: filepath.Join("testdata", "expected-online-with-no-http-port.cfg"),
			httpPort:     nil,
			peerPort:     newUint(12345),
			logPath:      nil,
		},
		{
			name:         "offline config with appendix",
			mode:         stellarCoreRunnerModeOffline,
			appendPath:   filepath.Join("testdata", "sample-appendix.cfg"),
			expectedPath: filepath.Join("testdata", "expected-offline-with-appendix-core.cfg"),
			httpPort:     newUint(6789),
			peerPort:     newUint(12345),
			logPath:      nil,
		},
		{
			name:         "offline config with extra fields in appendix",
			mode:         stellarCoreRunnerModeOffline,
			appendPath:   filepath.Join("testdata", "appendix-with-fields.cfg"),
			expectedPath: filepath.Join("testdata", "expected-offline-with-extra-fields.cfg"),
			httpPort:     newUint(6789),
			peerPort:     newUint(12345),
			logPath:      nil,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var err error
			var captiveCoreToml *CaptiveCoreToml
			params := CaptiveCoreTomlParams{
				NetworkPassphrase:  "Public Global Stellar Network ; September 2015",
				HistoryArchiveURLs: []string{"http://localhost:1170"},
				HTTPPort:           testCase.httpPort,
				PeerPort:           testCase.peerPort,
				LogPath:            testCase.logPath,
			}
			if testCase.appendPath != "" {
				captiveCoreToml, err = NewCaptiveCoreTomlFromFile(testCase.appendPath, params)
			} else {
				captiveCoreToml, err = NewCaptiveCoreToml(params)
			}
			assert.NoError(t, err)

			configBytes, err := generateConfig(captiveCoreToml, testCase.mode)
			assert.NoError(t, err)

			expectedByte, err := ioutil.ReadFile(testCase.expectedPath)
			assert.NoError(t, err)

			assert.Equal(t, string(configBytes), string(expectedByte))
		})
	}
}
