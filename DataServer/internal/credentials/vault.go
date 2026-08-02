// Package credentials provides the single server-side boundary for provider
// credentials. Plaintext material may exist only inside this package and only
// for the duration of an explicitly scoped operation.
//
// The Keyring (key management + AES-GCM seal/open) lives in the sibling
// file keyring_crypto.go.
package credentials

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"
)

var (
	ErrInvalidReference = errors.New("credentials: invalid credential_ref")
	ErrNotFound         = errors.New("credentials: credential not found")
	ErrRevoked          = errors.New("credentials: credential revoked")
	ErrExpired          = errors.New("credentials: credential expired")
	ErrScope            = errors.New("credentials: requested scope is not granted")
	ErrKeyUnavailable   = errors.New("credentials: encryption key unavailable")
)

type Material struct {
	AccessToken  string `json:"access_token,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
	ClientSecret string `json:"client_secret,omitempty"`
	APIKey       string `json:"api_key,omitempty"`
}

type StoredCredential struct {
	Ref           string
	Provider      string
	Owner         string
	Ciphertext    []byte
	KeyVersion    int
	Scopes        []string
	ExpiresAt     time.Time
	RotationDueAt time.Time
	RevokedAt     *time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
	LastUsedAt    *time.Time
}

type Repository interface {
	PutCredential(context.Context, StoredCredential) error
	GetCredential(context.Context, string) (*StoredCredential, error)
	UpdateCredential(context.Context, StoredCredential) error
	RevokeCredential(context.Context, string, time.Time) error
	RecordCredentialUse(context.Context, string, UsageEvent) error
}

type UsageEvent struct {
	WorkerID      string
	PublicationID string
	Scope         []string
	UsedAt        time.Time
	Success       bool
	ErrorCode     string
}

type AccessLease struct {
	CredentialRef string
	Provider      string
	WorkerID      string
	PublicationID string
	AccessToken   string
	TokenType     string
	Scopes        []string
	ExpiresAt     time.Time
}

type Vault struct {
	repo     Repository
	keys     *Keyring
	now      func() time.Time
	leaseTTL time.Duration
}

func NewVault(repo Repository, keys *Keyring) (*Vault, error) {
	if repo == nil || keys == nil {
		return nil, ErrKeyUnavailable
	}
	return &Vault{repo: repo, keys: keys, now: func() time.Time { return time.Now().UTC() }, leaseTTL: 15 * time.Minute}, nil
}

func (v *Vault) Put(ctx context.Context, provider, owner string, scopes []string, expiresAt, rotationDueAt time.Time, material Material) (string, error) {
	if v == nil || v.repo == nil || v.keys == nil {
		return "", ErrKeyUnavailable
	}
	ref, err := opaqueRef()
	if err != nil {
		return "", err
	}
	version, key, err := v.keys.currentKey()
	if err != nil {
		return "", err
	}
	plain, err := json.Marshal(material)
	if err != nil {
		return "", err
	}
	ciphertext, err := encrypt(key, plain)
	if err != nil {
		return "", err
	}
	now := v.now().UTC()
	record := StoredCredential{Ref: ref, Provider: strings.TrimSpace(provider), Owner: strings.TrimSpace(owner), Ciphertext: ciphertext, KeyVersion: version, Scopes: normalizeScopes(scopes), ExpiresAt: expiresAt.UTC(), RotationDueAt: rotationDueAt.UTC(), CreatedAt: now, UpdatedAt: now}
	if record.Provider == "" || record.Owner == "" {
		return "", errors.New("credentials: provider and owner are required")
	}
	if err := v.repo.PutCredential(ctx, record); err != nil {
		return "", err
	}
	return ref, nil
}

func (v *Vault) IssueAccessLease(ctx context.Context, ref, workerID, publicationID string, requestedScopes []string) (*AccessLease, error) {
	record, material, err := v.load(ctx, ref)
	if err != nil {
		return nil, err
	}
	now := v.now().UTC()
	scopes := normalizeScopes(requestedScopes)
	if record.RevokedAt != nil {
		_ = v.recordUse(ctx, ref, workerID, publicationID, scopes, now, false, "CREDENTIAL_REVOKED")
		return nil, ErrRevoked
	}
	if !record.ExpiresAt.IsZero() && !record.ExpiresAt.After(now) {
		_ = v.recordUse(ctx, ref, workerID, publicationID, nil, now, false, "CREDENTIAL_EXPIRED")
		return nil, ErrExpired
	}
	if !scopesWithin(scopes, record.Scopes) {
		_ = v.recordUse(ctx, ref, workerID, publicationID, scopes, now, false, "CREDENTIAL_SCOPE_DENIED")
		return nil, ErrScope
	}
	if err := v.recordUse(ctx, ref, workerID, publicationID, scopes, now, true, ""); err != nil {
		return nil, err
	}
	leaseExpires := now.Add(v.leaseTTL)
	if !record.ExpiresAt.IsZero() && record.ExpiresAt.Before(leaseExpires) {
		leaseExpires = record.ExpiresAt
	}
	return &AccessLease{CredentialRef: record.Ref, Provider: record.Provider, WorkerID: workerID, PublicationID: publicationID, AccessToken: material.AccessToken, TokenType: "Bearer", Scopes: scopes, ExpiresAt: leaseExpires}, nil
}

// RecordLeaseResult appends the provider outcome to the credential usage
// audit. The lease's access token is intentionally never persisted.
func (v *Vault) RecordLeaseResult(ctx context.Context, lease *AccessLease, success bool, errorCode string) error {
	if v == nil || lease == nil || !validRef(lease.CredentialRef) {
		return ErrInvalidReference
	}
	return v.recordUse(ctx, lease.CredentialRef, lease.WorkerID, lease.PublicationID, lease.Scopes, v.now().UTC(), success, strings.TrimSpace(errorCode))
}

func (v *Vault) recordUse(ctx context.Context, ref, workerID, publicationID string, scopes []string, at time.Time, success bool, errorCode string) error {
	return v.repo.RecordCredentialUse(ctx, ref, UsageEvent{WorkerID: workerID, PublicationID: publicationID, Scope: scopes, UsedAt: at, Success: success, ErrorCode: errorCode})
}

func (v *Vault) Revoke(ctx context.Context, ref string) error {
	if !validRef(ref) {
		return ErrInvalidReference
	}
	return v.repo.RevokeCredential(ctx, ref, v.now().UTC())
}

// ValidateReference verifies existence, decryptability and current validity
// without returning secret material.
func (v *Vault) ValidateReference(ctx context.Context, ref string) error {
	record, _, err := v.load(ctx, ref)
	if err != nil {
		return err
	}
	now := v.now().UTC()
	if record.RevokedAt != nil {
		return ErrRevoked
	}
	if !record.ExpiresAt.IsZero() && !record.ExpiresAt.After(now) {
		return ErrExpired
	}
	return nil
}

func (v *Vault) Rotate(ctx context.Context, ref string, material Material, expiresAt, rotationDueAt time.Time) error {
	record, _, err := v.load(ctx, ref)
	if err != nil {
		return err
	}
	version, key, err := v.keys.currentKey()
	if err != nil {
		return err
	}
	plain, err := json.Marshal(material)
	if err != nil {
		return err
	}
	ciphertext, err := encrypt(key, plain)
	if err != nil {
		return err
	}
	record.Ciphertext, record.KeyVersion, record.ExpiresAt, record.RotationDueAt, record.UpdatedAt = ciphertext, version, expiresAt.UTC(), rotationDueAt.UTC(), v.now().UTC()
	return v.repo.UpdateCredential(ctx, *record)
}

func (v *Vault) load(ctx context.Context, ref string) (*StoredCredential, Material, error) {
	if !validRef(ref) {
		return nil, Material{}, ErrInvalidReference
	}
	record, err := v.repo.GetCredential(ctx, ref)
	if errors.Is(err, ErrNotFound) || record == nil {
		return nil, Material{}, ErrNotFound
	}
	if err != nil {
		return nil, Material{}, err
	}
	key, err := v.keys.key(record.KeyVersion)
	if err != nil {
		return nil, Material{}, err
	}
	plain, err := decrypt(key, record.Ciphertext)
	if err != nil {
		return nil, Material{}, fmt.Errorf("credentials: decrypt %s: %w", ref, err)
	}
	var material Material
	if err := json.Unmarshal(plain, &material); err != nil {
		return nil, Material{}, err
	}
	return record, material, nil
}

func opaqueRef() (string, error) {
	raw := make([]byte, 18)
	if _, err := io.ReadFull(rand.Reader, raw); err != nil {
		return "", err
	}
	return "cred_" + hex.EncodeToString(raw), nil
}

var refRE = regexp.MustCompile(`^cred_[a-f0-9]{36}$`)

func validRef(ref string) bool { return refRE.MatchString(strings.TrimSpace(ref)) }

// ValidReference is the public opaque-reference shape check.
func ValidReference(ref string) bool { return validRef(ref) }

func normalizeScopes(scopes []string) []string {
	set := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		if scope = strings.TrimSpace(scope); scope != "" {
			set[scope] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for scope := range set {
		out = append(out, scope)
	}
	sort.Strings(out)
	return out
}

func scopesWithin(requested, granted []string) bool {
	set := make(map[string]struct{}, len(granted))
	for _, scope := range granted {
		set[scope] = struct{}{}
	}
	for _, scope := range requested {
		if _, ok := set[scope]; !ok {
			return false
		}
	}
	return true
}

// SecretDigest is safe for audit correlation and never returns secret data.
func SecretDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:8])
}
