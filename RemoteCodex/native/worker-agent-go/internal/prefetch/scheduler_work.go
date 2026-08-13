package prefetch

import (
	"container/heap"
	"time"

	"velox-shared/assetref"
	"velox-worker-agent/internal/downloader"
)

// runWorker drains the priority queue and executes one work item at a time
// until the scheduler's worker context is cancelled.
func (s *Scheduler) runWorker() {
	idleTicker := time.NewTicker(25 * time.Millisecond)
	defer idleTicker.Stop()
	for {
		item, resolver := s.nextWorkItem()
		if item != nil && resolver != nil {
			s.runWorkItem(item, resolver)
			continue
		}
		select {
		case <-s.workerCtx.Done():
			return
		case <-s.wake:
		case <-idleTicker.C:
		}
	}
}

func (s *Scheduler) nextWorkItem() (*workItem, *downloader.CacheResolver) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for attempts := s.queue.Len(); attempts > 0; attempts-- {
		item := heap.Pop(&s.queue).(*workItem)
		if !s.currentItemLocked(item) {
			continue
		}
		state := s.diskStateLocked()
		if state == diskCritical || (state == diskRestricted && item.job.Distance != 1) {
			heap.Push(&s.queue, item)
			continue
		}
		if s.resolver == nil {
			heap.Push(&s.queue, item)
			return nil, nil
		}
		// A single asset larger than the budget must remain pending too. The
		// previous s.bytes > 0 guard admitted an oversized first item and made
		// the byte budget depend on queue order.
		if item.asset.SizeBytes < 0 || s.bytes+item.asset.SizeBytes > s.cfg.ByteBudget {
			heap.Push(&s.queue, item)
			continue
		}
		s.bytes += item.asset.SizeBytes
		s.activePrefetch++
		return item, s.resolver
	}
	return nil, nil
}

func (s *Scheduler) runWorkItem(item *workItem, resolver *downloader.CacheResolver) {
	job, asset := item.job, item.asset
	startedAt := s.cfg.Now()
	bandwidth := s.cfg.MaxBandwidthBytesPerSecond
	if bandwidth > 0 {
		bandwidth /= int64(s.cfg.MaxConcurrent)
		if bandwidth == 0 {
			bandwidth = 1
		}
	}
	request := downloader.DownloadRequest{
		JobID: job.JobID, TaskID: job.TaskID, AssetKey: assetref.AssetKey(asset.AssetKey), AssetID: asset.AssetID,
		Role: downloader.RoleFromString(asset.Role), Source: "master_asset_bridge",
		SHA256: assetref.ContentHash(asset.SHA256), SizeBytes: asset.SizeBytes, MIMEType: asset.MIMEType,
		Priority:                   priorityForDistance(job.Distance),
		MaxBandwidthBytesPerSecond: bandwidth,
	}
	s.mu.Lock()
	active, queueDepth := s.activePrefetch, s.queue.Len()
	s.mu.Unlock()
	s.emit(Event{Name: "download_started", At: startedAt, PlanVersion: item.planVersion, JobID: job.JobID, TaskID: job.TaskID, AssetKey: asset.AssetKey, Distance: job.Distance, Generation: item.generation, QueuedAt: item.enqueuedAt, StartedAt: startedAt, QueueDepth: queueDepth, Active: active})
	if s.cfg.OnState != nil {
		s.cfg.OnState("requested", job, asset, nil)
	}
	resolved, err := resolver.Resolve(item.ctx, request)
	var protectionErr error
	if err == nil {
		// The canonical transferer commits the verified cache row before
		// Resolve returns. Install a protection that was pending because
		// the row did not exist when the plan arrived.
		protectionErr = s.installPendingProtection(asset.AssetKey)
	}
	if err == nil && !resolved.CacheHit {
		s.mu.Lock()
		s.prefetched[asset.AssetKey] = asset.SizeBytes
		if s.assetJobs[asset.AssetKey] == nil {
			s.assetJobs[asset.AssetKey] = make(map[string]struct{})
		}
		s.assetJobs[asset.AssetKey][job.JobID] = struct{}{}
		s.mu.Unlock()
		if s.cfg.OnState != nil {
			s.cfg.OnState("downloaded", job, asset, nil)
		}
	}
	if err == nil && s.ram != nil {
		s.mu.Lock()
		hint, hinted := s.hints[asset.AssetKey]
		s.mu.Unlock()
		if hinted && hint.FutureRefCount >= s.cfg.RAMMinFutureRefs && hint.NextUseDistance <= s.cfg.RAMMaxNextUseDistance {
			_ = s.ram.Put(item.ctx, request, downloader.DownloadedAsset{AssetKey: request.AssetKey, AssetID: request.AssetID, LocalPath: resolved.LocalPath, SHA256: resolved.SHA256, SizeBytes: asset.SizeBytes})
		}
	}
	s.releaseWork(asset.SizeBytes)
	readyAt := s.cfg.Now()
	if err == nil && protectionErr == nil {
		s.mu.Lock()
		if s.readyAtByJob[job.JobID] == nil {
			s.readyAtByJob[job.JobID] = make(map[string]readyRecord)
		}
		s.readyAtByJob[job.JobID][asset.AssetKey] = readyRecord{at: readyAt, distance: job.Distance}
		s.mu.Unlock()
	}
	if s.currentItem(item) {
		if s.cfg.OnState != nil {
			if err != nil {
				s.cfg.OnState("failed", job, asset, err)
			} else if protectionErr != nil {
				s.cfg.OnState("protection_failed", job, asset, protectionErr)
			} else {
				s.cfg.OnState("ready", job, asset, nil)
			}
		}
		s.mu.Lock()
		active, queueDepth := s.activePrefetch, s.queue.Len()
		s.mu.Unlock()
		eventName := "asset_ready"
		if protectionErr != nil && err == nil {
			eventName = "asset_ready_unprotected"
		}
		event := Event{Name: eventName, At: readyAt, PlanVersion: item.planVersion, JobID: job.JobID, TaskID: job.TaskID, AssetKey: asset.AssetKey, Distance: job.Distance, Generation: item.generation, QueuedAt: item.enqueuedAt, StartedAt: startedAt, ReadyAt: readyAt, QueueDepth: queueDepth, Active: active}
		if err != nil {
			event.ErrorMessage = err.Error()
		} else if protectionErr != nil {
			event.ErrorMessage = protectionErr.Error()
		}
		s.emit(event)
	}
}

func (s *Scheduler) diskStateLocked() diskPressureState {
	if s.cfg.DiskUsagePercent == nil {
		return diskNormal
	}
	usage := s.cfg.DiskUsagePercent()
	switch s.state {
	case diskCritical:
		if usage < s.cfg.DiskRecoveryPercent {
			s.state = diskNormal
		}
	case diskRestricted:
		if usage >= s.cfg.DiskCriticalPercent {
			s.state = diskCritical
		} else if usage < s.cfg.DiskRecoveryPercent {
			s.state = diskNormal
		}
	default:
		if usage >= s.cfg.DiskCriticalPercent {
			s.state = diskCritical
		} else if usage >= s.cfg.DiskRestrictedPercent {
			s.state = diskRestricted
		}
	}
	return s.state
}

func (s *Scheduler) releaseWork(n int64) {
	s.mu.Lock()
	s.bytes -= n
	if s.bytes < 0 {
		s.bytes = 0
	}
	if s.activePrefetch > 0 {
		s.activePrefetch--
	}
	s.mu.Unlock()
	s.signalWork()
}
