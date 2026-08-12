// Package telemetry / network_sampler.go
//
// Network sampling split out of resource_sampler.go as part of the
// per-domain refactor (cpu / memory / disk / network / process / host
// + resource_sampler.go facade). The /proc/net/dev reader picks the
// primary interface (highest rx_bytes, deterministic tie-break by
// name) and skips loopback + virtual interfaces (docker/veth/br-*).
//
// All values are CUMULATIVE — no delta math on the worker. Master F2
// LastSeenResources does the per-beat math.
package telemetry

import (
	"errors"
	"path/filepath"
	"strconv"
	"strings"
)

// netCumulatives holds /proc/net/dev fields for one interface.
type netCumulatives struct {
	rxBytes     int64
	txBytes     int64
	retransmits int64
}

// readProcNetDevPrimary picks the interface with the highest rx_bytes,
// skipping `lo`. Operates on cumulative numbers (the wire contract).
func (s *Sampler) readProcNetDevPrimary() (netCumulatives, error) {
	data, err := readFile(filepath.Join(s.procRoot, "net", "dev"))
	if err != nil {
		return netCumulatives{}, err
	}
	var best netCumulatives
	var bestName string
	for _, line := range strings.Split(string(data), "\n") {
		// Header / blank lines. Skip.
		idx := strings.IndexByte(line, ':')
		if idx <= 0 {
			continue
		}
		name := strings.TrimSpace(line[:idx])
		if name == "" || name == "lo" || name == "Inter-|" || name == " face" {
			continue
		}
		// Skip obviously-virtual names: docker*, veth*, br-*.
		if strings.HasPrefix(name, "docker") || strings.HasPrefix(name, "veth") || strings.HasPrefix(name, "br-") {
			continue
		}
		rest := strings.TrimSpace(line[idx+1:])
		cols := strings.Fields(rest)
		if len(cols) < 10 {
			continue
		}
		rxBytes, _ := strconv.ParseInt(cols[0], 10, 64)
		// /proc/net/dev schema: 8 receive fields (bytes, packets, errs,
		// drop, fifo, frame, compressed, multicast) + 8 transmit fields
		// (bytes, packets, errs, drop, fifo, colls, carrier, compressed).
		// cols[0] = rx_bytes, cols[8] = rx_multicast (zero on most
		// interfaces), cols[9] = tx_bytes. We must read [9] for tx.
		txBytes, _ := strconv.ParseInt(cols[9], 10, 64)
		// Pick the interface with the largest rx_bytes on this beat.
		// Ties broken by name lexicographic order for determinism.
		if rxBytes > best.rxBytes || (rxBytes == best.rxBytes && name < bestName) {
			best = netCumulatives{rxBytes: rxBytes, txBytes: txBytes}
			bestName = name
		}
	}
	if bestName == "" {
		return netCumulatives{}, errors.New("proc/net/dev: no non-virtual interface")
	}
	return best, nil
}
