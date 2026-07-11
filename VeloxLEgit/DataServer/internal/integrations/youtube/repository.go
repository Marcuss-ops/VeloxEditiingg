package youtube

// YouTubeRepository is a strict type alias of Repository.
//
// PR-YT-REPO: YouTubeStore and StorageStore are unified into the
// canonical youtube.Repository declared in service.go. YouTubeRepository
// and YouTubeStore are both kept as type aliases so any caller that
// held the older names continues to compile.
//
// Migration status:
//
//   - All production callers in DataServer/cmd/server, DataServer/internal/app
//     and DataServer/internal/handlers now depend on Repository
//     indirectly via *Service (which holds `repo Repository`).
//   - Tests that embed YouTubeStore as a nil-field continue to compile
//     because `type YouTubeStore = Repository` is a pure alias.
//
// New code MUST spell the canonical name Repository directly.
// YouTubeRepository and YouTubeStore are kept only as transition
// aliases for the post-PR15.4 cleanup window.
type YouTubeRepository = Repository

// Compile-time assertion that *Service.repo (interface-typed field)
// is reachable through the alias path. Verifies the alias chain:
// YouTubeRepository = Repository, so a Repository-typed field can hold
// any value assignable through either alias.
var _ YouTubeRepository = Repository(nil)
