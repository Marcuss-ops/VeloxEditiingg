package credentials

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"velox-server/internal/config"
)

// keyring_crypto.go owns the Keyring (key management + AES-GCM
// seal/open primitives). The Vault domain logic lives in vault.go.

type Keyring struct {
	mu      sync.RWMutex
	keys    map[int][]byte
	current int
}

func NewKeyring(current int, keys map[int][]byte) (*Keyring, error) {
	if current <= 0 || len(keys) == 0 {
		return nil, ErrKeyUnavailable
	}
	copyKeys := make(map[int][]byte, len(keys))
	for version, key := range keys {
		if version <= 0 || len(key) != 32 {
			return nil, fmt.Errorf("credentials: key version %d must be 32 bytes", version)
		}
		copyKeys[version] = append([]byte(nil), key...)
	}
	if _, ok := copyKeys[current]; !ok {
		return nil, ErrKeyUnavailable
	}
	return &Keyring{keys: copyKeys, current: current}, nil
}

// LoadKeyring reads the current key from VELOX_CREDENTIAL_KEY or its _FILE
// sibling. The value may be raw base64 or hexadecimal. Optional historical
// keys are loaded from VELOX_CREDENTIAL_KEY_<version>.
func LoadKeyring() (*Keyring, error) {
	current := 1
	if value := strings.TrimSpace(config.Getenv("VELOX_CREDENTIAL_KEY_VERSION")); value != "" {
		if _, err := fmt.Sscanf(value, "%d", &current); err != nil || current <= 0 {
			return nil, ErrKeyUnavailable
		}
	}
	key, err := readKeyEnv("VELOX_CREDENTIAL_KEY")
	if err != nil {
		return nil, err
	}
	keys := map[int][]byte{current: key}
	for version := 1; version <= 32; version++ {
		if version == current {
			continue
		}
		if historical, readErr := readKeyEnv(fmt.Sprintf("VELOX_CREDENTIAL_KEY_%d", version)); readErr == nil {
			keys[version] = historical
		}
	}
	return NewKeyring(current, keys)
}

func readKeyEnv(name string) ([]byte, error) {
	value := strings.TrimSpace(config.Getenv(name))
	if value == "" {
		file := strings.TrimSpace(config.Getenv(name + "_FILE"))
		if file != "" {
			data, err := os.ReadFile(file)
			if err != nil {
				return nil, fmt.Errorf("credentials: read %s: %w", name, err)
			}
			value = strings.TrimSpace(string(data))
		}
	}
	if value == "" {
		return nil, ErrKeyUnavailable
	}
	if decoded, err := base64.RawStdEncoding.DecodeString(value); err == nil && len(decoded) == 32 {
		return decoded, nil
	}
	if decoded, err := hex.DecodeString(value); err == nil && len(decoded) == 32 {
		return decoded, nil
	}
	if len(value) == 32 {
		return []byte(value), nil
	}
	return nil, ErrKeyUnavailable
}

func (k *Keyring) currentKey() (int, []byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	key, ok := k.keys[k.current]
	if !ok {
		return 0, nil, ErrKeyUnavailable
	}
	return k.current, append([]byte(nil), key...), nil
}

func (k *Keyring) key(version int) ([]byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	key, ok := k.keys[version]
	if !ok {
		return nil, ErrKeyUnavailable
	}
	return append([]byte(nil), key...), nil
}

// Seal encrypts bytes with the current key and returns the key version needed
// to decrypt them. Callers persist only the ciphertext and version.
func (k *Keyring) Seal(plain []byte) ([]byte, int, error) {
	version, key, err := k.currentKey()
	if err != nil {
		return nil, 0, err
	}
	ciphertext, err := encrypt(key, plain)
	if err != nil {
		return nil, 0, err
	}
	return ciphertext, version, nil
}

// Open decrypts bytes using a specific key version. Historical key versions
// remain readable during rotation; new values always use the current key.
func (k *Keyring) Open(version int, ciphertext []byte) ([]byte, error) {
	key, err := k.key(version)
	if err != nil {
		return nil, err
	}
	return decrypt(key, ciphertext)
}

func encrypt(key []byte, plain []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plain, nil), nil
}

func decrypt(key []byte, encrypted []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(encrypted) < gcm.NonceSize() {
		return nil, errors.New("credentials: ciphertext too short")
	}
	return gcm.Open(nil, encrypted[:gcm.NonceSize()], encrypted[gcm.NonceSize():], nil)
}
