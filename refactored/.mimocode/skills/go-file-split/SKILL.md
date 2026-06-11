---
name: go-file-split
description: "Split large Go source files into smaller modules while preserving package cohesion. Includes discovery, analysis, splitting, build verification, and commit."
---

# Go Large File Splitting

Split oversized Go source files into multiple smaller files within the same package. Encodes the proven workflow from 30+ file splits across the VeloxLEgit codebase.

## When to use

- A single Go file exceeds ~500 lines
- The user asks to "split", "modularize", "break up", or "spezzare" large files
- A code review flags a file as too large

## Procedure

### Step 1 — Discovery (find targets)

Find Go files exceeding the line threshold. Use the **main project directory** (e.g., `refactored/DataServer/`), NOT stale copies.

```bash
find <ROOT>/DataServer -name '*.go' -not -path '*/vendor/*' -not -path '*/worker_downloads/*' -not -path '*/build/*' | xargs wc -l | sort -rn | head -30
```

**CRITICAL**: Always exclude `worker_downloads/`, `build/`, and `vendor/` directories. These contain stale copies with inflated line counts. Always verify the file exists in the main dir before processing.

### Step 2 — Analyze structure

For each target file, read it completely and produce a structured analysis:

1. List every function/method with line ranges
2. Identify logical groupings (domain concerns that belong together)
3. Flag dead code (unused functions, variables, redundant implementations)
4. Determine the best split pattern (see below)

Use an **explore subagent** for analysis if handling multiple files — explore subagents CAN successfully read and analyze files. Do NOT use general subagents for this.

### Step 3 — Choose split pattern

Select the pattern that fits the file's structure:

| Pattern | When to use | Example |
|---------|-------------|---------|
| **Module-manager** | Central orchestrator/coordinator file with many domain concerns | `orchestrator.go` → `orchestrator.go` + `orchestrator_types.go` + `orchestrator_queries.go` |
| **Entity-per-file** | Store/DB files with operations on distinct entities/tables | `sqlite_darkeditor.go` → `projects.go`, `folders.go`, `assets.go`, `schema.go` |
| **Handler-grouping** | HTTP handlers file with endpoints grouped by API domain | `handlers.go` → `handlers_images.go`, `handlers_projects.go`, `handlers_logs.go` |
| **Types/queries split** | Complex state machine separating type defs from mutation methods | `registry.go` → `registry.go` (types) + `registry_queries.go` + `registry_commands.go` |

### Step 4 — Execute splits

For each file, perform:

1. **Read** the entire original file
2. **Plan** which functions/types/vars go to which new file
3. **Write** each new file with:
   - Same `package` declaration
   - Only the imports that file actually needs (DO NOT copy all imports)
   - Functions/types moved exactly as-is (no refactoring during the split)
4. **Delete** the original file if all content was distributed
5. **Verify** the build passes for that package:

```bash
cd <ROOT>/DataServer && go build ./internal/<package_path>/
```

**IMPORTANT**: Move code exactly as-is. Do NOT refactor, rename, or optimize during the split. The goal is purely structural decomposition.

### Step 5 — Full build verification

After all files are split:

```bash
cd <ROOT>/DataServer && go build ./... 2>&1 | head -30
```

Fix any import errors (missing imports, unused imports). This is the most common post-split issue.

### Step 6 — Commit

```bash
cd <ROOT> && git add -A && git commit -m "Split <file> into N files" && git push origin main
```

## Critical Gotchas

1. **Stale copy paths**: `DataServer/data/worker_downloads/code/refactored/` contains OLD copies of source files from before splitting. NEVER edit these. Always work in the main `refactored/DataServer/internal/` directory.

2. **General subagents fail at file splitting**: They return "success" with 0 turns and produce no output. Do file splitting manually using Write/Edit tools directly. Only use explore subagents for the analysis step.

3. **`find` returns stale copies first**: When searching for files by name, stale `worker_downloads/` paths match first. Always filter them out explicitly.

4. **Build errors are usually imports**: After splitting, the most common issue is missing or unused imports in the new files. Fix with Edit, not by re-copying.

5. **Test files**: Keep test files (e.g., `registry_test.go`) whole unless they exceed 1000+ lines. Tests are typically cohesive and don't benefit from splitting.

6. **Build artifacts**: `build/` directories contain auto-generated files (CMake outputs, etc.) — exclude from analysis.

## Example split plans

### Handler-grouping pattern
```
handlers.go (697 lines) →
  handlers_images.go   — UploadImage, ApplyFilter, TransformImage, ExportImage, GenerateImage, UpscaleImage
  handlers_projects.go — CRUD for projects (Create, Update, Delete, List, Get)
  handlers_logs.go     — Logging handlers (ListLogs, GetLogStats, StreamLogs)
```

### Module-manager pattern
```
orchestrator.go (804 lines) →
  orchestrator.go          — Orchestrator struct, constructor, main orchestration loop
  orchestrator_types.go    — StepStatus, JobStep, MultiStepJob, OrchestratorConfig
  orchestrator_queries.go  — GetJobStatus, ListSteps, GetStepResult
  orchestrator_processing.go — ProcessStep, HandleStepResult, RetryLogic
  orchestrator_pipelines.go  — Pipeline definitions, step ordering
```

### Entity-per-file pattern
```
sqlite_darkeditor.go (866 lines) →
  projects.go       — CreateProject, GetProject, UpdateProject, DeleteProject, ListProjects
  folders.go        — CreateFolder, GetFolder, MoveFolder, ListFolders
  assets.go         — UploadAsset, GetAsset, DeleteAsset, ListAssets
  templates.go      — CRUD for templates
  temp.go           — Temporary file operations
  generations.go    — Generation tracking (create, update status, list)
  schema.go         — DDL, migrations, table definitions
```
