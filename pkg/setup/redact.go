package setup

import (
	"fmt"
	"strings"

	"github.com/goccy/go-yaml"
)

// redactedPlaceholder is what replaces the value of a sensitive config field.
const redactedPlaceholder = "**REDACTED**"

// sensitiveKeyParts are matched as substrings against lower-cased YAML keys. Any
// match has its value replaced before the config is logged. Substring matching is
// deliberate so that credential fields added by future sinks (secretAccessKey,
// clientSecret, saslPassword, ...) are covered without touching this list.
var sensitiveKeyParts = []string{
	"password",
	"token",
	"apikey",
	"api_key",
	"secret",
	"credential",
	"privatekey",
	"private_key",
	"passphrase",
	"webhook",
}

// isSensitiveKey reports whether a config key holds a credential.
func isSensitiveKey(key string) bool {
	k := strings.ToLower(strings.ReplaceAll(key, "-", ""))
	for _, part := range sensitiveKeyParts {
		if strings.Contains(k, part) {
			return true
		}
	}
	return false
}

// redactValue walks a decoded YAML tree and masks every value stored under a
// sensitive key, at any depth.
func redactValue(node interface{}) interface{} {
	switch v := node.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(v))
		for key, value := range v {
			if isSensitiveKey(key) {
				out[key] = redactedPlaceholder
				continue
			}
			out[key] = redactValue(value)
		}
		return out
	case map[interface{}]interface{}:
		out := make(map[interface{}]interface{}, len(v))
		for key, value := range v {
			if k, ok := key.(string); ok && isSensitiveKey(k) {
				out[key] = redactedPlaceholder
				continue
			}
			out[key] = redactValue(value)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(v))
		for i, item := range v {
			out[i] = redactValue(item)
		}
		return out
	default:
		return node
	}
}

// RedactedConfigString renders cfg as YAML with all credential fields masked. The
// raw config must never be logged: main expands environment variables into it
// before parsing, so sink passwords, API keys and tokens are present in cleartext
// and would otherwise be shipped to whatever collects the pod's stdout.
func RedactedConfigString(cfg interface{}) string {
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Sprintf("<cannot render config: %v>", err)
	}

	var tree interface{}
	if err := yaml.Unmarshal(raw, &tree); err != nil {
		return fmt.Sprintf("<cannot render config: %v>", err)
	}

	out, err := yaml.Marshal(redactValue(tree))
	if err != nil {
		return fmt.Sprintf("<cannot render config: %v>", err)
	}

	return string(out)
}
