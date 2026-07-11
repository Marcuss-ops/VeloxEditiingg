#!/usr/bin/env python3
"""Resolve git merge conflicts."""
import re, os, sys

base = "."

files_config = {
    "DataServer/cmd/server/bootstrap.go": [],
    "DataServer/cmd/server/migrate.go": [],
    "DataServer/cmd/server/router.go": [],
    "DataServer/internal/handlers/remote/workers/uploads/video_upload_completed_test.go": [],
    "DataServer/internal/handlers/server/script/handler_test.go": [],
    "DataServer/internal/queue/transition.go": [],
}

for f in files_config:
    path = os.path.join(base, f)
    with open(path) as fh:
        content = fh.read()
    original = content
    
    # bootstrap.go: keep grpcserver + workerhandlers + lifecycle
    content = content.replace(
        '<<<<<<< HEAD\n\t"velox-server/internal/grpcserver"\n=======\n\tworkerhandlers "velox-server/internal/handlers/remote/workers"\n\t"velox-server/internal/handlers/remote/workers/lifecycle"\n>>>>>>> bf27e976',
        '\t"velox-server/internal/grpcserver"\n\tworkerhandlers "velox-server/internal/handlers/remote/workers"\n\t"velox-server/internal/handlers/remote/workers/lifecycle"'
    )
    
    # migrate.go: keep our version
    content = re.sub(
        r'<<<<<<< HEAD\n// runMigrateOAuthJSON.*?=======\n// runMigrateOAuthJSON',
        '// runMigrateOAuthJSON',
        content,
        flags=re.DOTALL
    )
    
    # router.go imports: keep both workersuploads + integrationsDrive
    content = content.replace(
        '<<<<<<< HEAD\n\tworkersuploads "velox-server/internal/handlers/remote/workers/uploads"\n=======\n\tuploads "velox-server/internal/handlers/remote/workers/uploads"\n\tintegrationsDrive "velox-server/internal/integrations/drive"\n>>>>>>> bf27e976',
        '\tworkersuploads "velox-server/internal/handlers/remote/workers/uploads"\n\tintegrationsDrive "velox-server/internal/integrations/drive"'
    )
    # router.go RegisterV1Routes: keep our version with driveService
    content = content.replace(
        '<<<<<<< HEAD\n\tapi.RegisterV1Routes(r, cfg, deps.fileQ, deps.reg, jobAPI, jobSubmitHandler, deps.workersRepo, deps.sqliteStore, deps.workerUpdateHandler, youtubeService, nil, ansibleHandlers) // TODO: wire driveService\n=======\n\tvar driveService *integrationsDrive.Service\n\tif deps.driveModule != nil {\n\t\tdriveService = deps.driveModule.Service()\n\t}\n\tapi.RegisterV1Routes(r, cfg, deps.fileQ, deps.reg, jobAPI, jobSubmitHandler, deps.workersRepo, deps.sqliteStore, deps.workerUpdateHandler, youtubeService, driveService, ansibleHandlers)\n>>>>>>> bf27e976',
        '\tvar driveService *integrationsDrive.Service\n\tif deps.driveModule != nil {\n\t\tdriveService = deps.driveModule.Service()\n\t}\n\tapi.RegisterV1Routes(r, cfg, deps.fileQ, deps.reg, jobAPI, jobSubmitHandler, deps.workersRepo, deps.sqliteStore, deps.workerUpdateHandler, youtubeService, driveService, ansibleHandlers)'
    )
    # router.go chunked upload: keep HEAD's workersuploads prefix
    content = content.replace(
        '<<<<<<< HEAD\n\tr.POST("/api/v1/video/chunked/init", workersuploads.InitChunkedUpload())\n\tr.POST("/api/v1/video/chunked/:job_id/:chunk_index", workersuploads.UploadChunk(cfg))\n\tr.POST("/api/v1/video/chunked/:job_id/complete", workersuploads.CompleteChunkedUpload(cfg, deps.fileQ))\n=======\n\tr.POST("/api/v1/video/chunked/init", uploads.InitChunkedUpload())\n\tr.POST("/api/v1/video/chunked/:job_id/:chunk_index", uploads.UploadChunk(cfg))\n\tr.POST("/api/v1/video/chunked/:job_id/complete", uploads.CompleteChunkedUpload(cfg, deps.fileQ))\n>>>>>>> bf27e976',
        '\tr.POST("/api/v1/video/chunked/init", workersuploads.InitChunkedUpload())\n\tr.POST("/api/v1/video/chunked/:job_id/:chunk_index", workersuploads.UploadChunk(cfg))\n\tr.POST("/api/v1/video/chunked/:job_id/complete", workersuploads.CompleteChunkedUpload(cfg, deps.fileQ))'
    )
    
    # video_upload_completed_test.go config: keep HEAD's version with VideosDir
    content = content.replace(
        '<<<<<<< HEAD\n\t\tRuntime: config.RuntimeConfig{\n\t\t\tDataDir:   tempDir,\n\t\t\tVideosDir: filepath.Join(tempDir, "completed_videos"),\n\t\t},\n=======\n\t\tRuntime: config.RuntimeConfig{DataDir: tempDir},\n>>>>>>> bf27e976',
        '\t\tRuntime: config.RuntimeConfig{\n\t\t\tDataDir:   tempDir,\n\t\t\tVideosDir: filepath.Join(tempDir, "completed_videos"),\n\t\t},'
    )
    # video_upload_completed_test.go DeliveryTarget: keep our version
    content = content.replace(
        '<<<<<<< HEAD\n\tmaybeAutoUploadDrive(q, driveSvc, tempDir, jobID, map[string]interface{}{}, videoPath, nil)\n=======\n\ttargets := []store.DeliveryTarget{{\n\t\tTargetType: "drive",\n\t\tStatus:     "pending",\n\t\tConfig:     `{"folder_id":"drive-folder-it"}`,\n\t}}\n\tmaybeAutoUploadDrive(q, driveSvc, tempDir, jobID, map[string]interface{}{}, videoPath, targets)\n>>>>>>> bf27e976',
        '\ttargets := []store.DeliveryTarget{{\n\t\tTargetType: "drive",\n\t\tStatus:     "pending",\n\t\tConfig:     `{"folder_id":"drive-folder-it"}`,\n\t}}\n\tmaybeAutoUploadDrive(q, driveSvc, tempDir, jobID, map[string]interface{}{}, videoPath, targets)'
    )
    
    # script/handler_test.go: keep HEAD's config with VideosDir
    content = content.replace(
        '<<<<<<< HEAD\n\t\tRuntime: config.RuntimeConfig{\n\t\t\tDataDir:   tempDir,\n\t\t\tVideosDir: filepath.Join(tempDir, "videos"),\n\t\t},\n\t\tDatabase: config.DatabaseConfig{\n\t\t\tDBPath: dbPath,\n\t\t},\n=======\n\t\tRuntime:  config.RuntimeConfig{DataDir: tempDir},\n\t\tDatabase: config.DatabaseConfig{DBPath: dbPath},\n>>>>>>> bf27e976',
        '\t\tRuntime: config.RuntimeConfig{\n\t\t\tDataDir:   tempDir,\n\t\t\tVideosDir: filepath.Join(tempDir, "videos"),\n\t\t},\n\t\tDatabase: config.DatabaseConfig{\n\t\t\tDBPath: dbPath,\n\t\t},'
    )
    
    # transition.go: keep our imports (sort, sync)
    content = content.replace(
        '<<<<<<< HEAD\n=======\n\t"sort"\n\t"sync"\n>>>>>>> bf27e976',
        '\t"sort"\n\t"sync"'
    )
    
    # Clean up any remaining conflict markers
    content = re.sub(r'<<<<<<< HEAD\n', '', content)
    content = re.sub(r'=======\n', '', content)
    content = re.sub(r'>>>>>>> [^\n]+\n', '', content)
    
    if content != original:
        with open(path, 'w') as fh:
            fh.write(content)
        print(f"Resolved: {f}")
    else:
        print(f"No changes: {f}")

print("Done!")
