package fleet

import (
	"fmt"
	"os"
	"syscall"
)

// ValidateSSHMaterial verifies the local control-plane credential contract
// used by the FleetController. It deliberately checks the effective process
// identity, not merely root readability: a root-owned 0600 key is unusable by
// the unprivileged Master container and must fail readiness before rollout.
func ValidateSSHMaterial(keyPath, knownHostsPath string) error {
	if keyPath == "" {
		keyPath = DefaultSSHKeyPath
	}
	if knownHostsPath == "" {
		knownHostsPath = DefaultKnownHostsPath
	}
	keyInfo, err := os.Stat(keyPath)
	if err != nil {
		return fmt.Errorf("ssh private key: %w", err)
	}
	if keyInfo.Mode().Perm() != 0600 {
		return fmt.Errorf("ssh private key %s mode=%04o want=0600", keyPath, keyInfo.Mode().Perm())
	}
	keyStat, ok := keyInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("ssh private key %s: unsupported file metadata", keyPath)
	}
	if uint32(keyStat.Uid) != uint32(os.Getuid()) || uint32(keyStat.Gid) != uint32(os.Getgid()) {
		return fmt.Errorf("ssh private key %s owner=%d:%d want=%d:%d", keyPath, keyStat.Uid, keyStat.Gid, os.Getuid(), os.Getgid())
	}
	knownInfo, err := os.Stat(knownHostsPath)
	if err != nil {
		return fmt.Errorf("ssh known_hosts: %w", err)
	}
	if knownInfo.IsDir() || knownInfo.Mode().Perm()&0444 == 0 {
		return fmt.Errorf("ssh known_hosts %s is not readable", knownHostsPath)
	}
	key, err := os.Open(keyPath)
	if err != nil {
		return fmt.Errorf("ssh private key %s is not readable by runtime: %w", keyPath, err)
	}
	_ = key.Close()
	known, err := os.Open(knownHostsPath)
	if err != nil {
		return fmt.Errorf("ssh known_hosts %s is not readable by runtime: %w", knownHostsPath, err)
	}
	_ = known.Close()
	return nil
}
