package setup

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_RedactedConfigString_MasksCredentials(t *testing.T) {
	configBytes := []byte(`
logLevel: info
receivers:
  - name: "os"
    opensearch:
      hosts:
        - "https://opensearch.example:9200"
      username: "svc-events"
      password: "hunter2-should-not-appear"
  - name: "es"
    elasticsearch:
      hosts:
        - "https://elastic.example:9200"
      apiKey: "apikey-should-not-appear"
  - name: "slack"
    slack:
      token: "xoxb-should-not-appear"
      channel: "#alerts"
  - name: "og"
    opsgenie:
      apiKey: "opsgenie-should-not-appear"
`)

	config, err := ParseConfigFromBytes(configBytes)
	assert.NoError(t, err)

	out := RedactedConfigString(&config)

	for _, secret := range []string{
		"hunter2-should-not-appear",
		"apikey-should-not-appear",
		"xoxb-should-not-appear",
		"opsgenie-should-not-appear",
	} {
		assert.NotContains(t, out, secret, "credential leaked into the loggable config")
	}

	// Non-sensitive fields must survive so the dump stays useful for debugging.
	assert.Contains(t, out, "https://opensearch.example:9200")
	assert.Contains(t, out, "svc-events")
	assert.Contains(t, out, "#alerts")
	assert.True(t, strings.Contains(out, redactedPlaceholder))
}

func Test_IsSensitiveKey(t *testing.T) {
	sensitive := []string{
		"password", "Password", "token", "apiKey", "api_key", "secretAccessKey",
		"clientSecret", "credentials", "privateKey", "passphrase", "webhookURL",
	}
	for _, k := range sensitive {
		assert.True(t, isSensitiveKey(k), "%q should be treated as sensitive", k)
	}

	notSensitive := []string{"hosts", "index", "indexFormat", "channel", "name", "layout", "region"}
	for _, k := range notSensitive {
		assert.False(t, isSensitiveKey(k), "%q should not be redacted", k)
	}
}
