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

// LoadKeyring builds a keyring from the one captured bootstrap snapshot.
// Key files are read here because their contents are secret material, while
// their paths and inline values are selected by internal/config.
func LoadKeyring(cfg config.CredentialsConfig) (*Keyring, error) {
	current := cfg.CurrentVersion
	if current <= 0 {
		current = 1
	}
	key, err := readKeyConfig(cfg.Current)
	if err != nil {
		return nil, err
	}
	keys := map[int][]byte{current: key}
	for version, historical := range cfg.Historical {
		if version == current {
			continue
		}
		if decoded, readErr := readKeyConfig(historical); readErr == nil {
			keys[version] = decoded
		}
	}
	return NewKeyring(current, keys)
}

func readKeyConfig(value config.CredentialKeyConfig) ([]byte, error) {
	encoded := strings.TrimSpace(value.Value)
	if encoded == "" && strings.TrimSpace(value.File) != "" {
		data, err := os.ReadFile(strings.TrimSpace(value.File))
		if err != nil {
			return nil, fmt.Errorf("credentials: read key file: %w", err)
		}
		encoded = strings.TrimSpace(string(data))
	}
	if encoded == "" {
		return nil, ErrKeyUnavailable
	}
	if decoded, err := base64.RawStdEncoding.DecodeString(encoded); err == nil && len(decoded) == 32 {
		return decoded, nil
	}
	if decoded, err := hex.DecodeString(encoded); err == nil && len(decoded) == 32 {
		return decoded, nil
	}
	if len(encoded) == 32 {
		return []byte(encoded), nil
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
