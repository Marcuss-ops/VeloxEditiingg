# YouTube Handlers Refactor Map

This report analyzes every function inside [internal/handlers/server/youtube/](file:///c:/Users/pater/Pyt/VeloxEditing/DataServer/internal/handlers/server/youtube/) and classifies them into:
- **(a) Pure HTTP Wrapper**: Handles request parsing, response formatting, status codes.
- **(b) Business Logic**: Orchesrates business rules, fetches API data, computes values.
- **(c) Cache/ORM to Eliminate**: Duplicated local persistence or cache layers that should be unified under standard store/integrations.

---

## 1. youtube_thumbnails.go

- **DownloadThumbnailHandler**: **(b) Business Logic**. Downloads thumbnail files, performs Content-Type checks, and normalizes file extensions.
- **ThumbnailAPIHandler**: **(a) Pure HTTP Wrapper**. Simple string template formatter returning JSON.

---

## 2. youtube_content.go

- **ScrapeToolHandler**: **(b) Business Logic**. Parses request JSON, extracts the video ID via helper, calls `apiClient.SearchVideos`, and formats responses.
- **VideoInfoHandler**: **(b) Business Logic**. Fetches video metadata via `apiClient.SearchVideos` and formats returns.
- **GenerateScriptHandler**: **(b) Business Logic**. Mock script generator formatting a Markdown template based on Query, Language, and Timestamp parameters.

---

## 3. youtube_discovery.go

- **ViralSearchHandler**: **(b) Business Logic**. Calculates date boundaries, parameters, and makes API calls.
- **DiscoveryHandler**: **(b) Business Logic**. Invokes API client search wrapper.
- **SimilarChannelsHandler**: **(b) Business Logic**. Performs channel similarity heuristics, filters and deduplicates based on `seen` mapping.
- **AutoSimilarChannelsHandler**: **(b) Business Logic / (c) Cache/ORM**. Loads all group data from local files, aggregates keywords, triggers similar search, and sorts results by velocity.
- **TrendsHandler**: **(b) Business Logic**. Normalizes trend parameters, queries API, and formats trend topics.
- **AIDigestHandler**: **(b) Business Logic**. Queries trending videos, builds weekly text summary digests.

---

## 4. youtube_feed.go

- **GetVideoFeedHandler**: **(b) Business Logic / (c) Cache/ORM**. Reads groups, aggregates channels, resolves limits/dates, makes concurrent API fetches, sorts feed, and updates a local cache.
- **RefreshAllGroupsFeed**: **(b) Business Logic**. Triggers re-scraping of feeds across all configured groups.
- **refreshGroupFeed**: **(b) Business Logic / (c) Cache/ORM**. Feeds channel items, updates local feed cache.
- **RefreshFeedHandler**: **(a) Pure HTTP Wrapper**. Sets up request timeout and dispatches to `RefreshAllGroupsFeed`.
- **ResolveChannelHandler**: **(b) Business Logic**. Dispatches url resolving to API client.
- **TrendingNewsHandler**: **(b) Business Logic**. Interacts with `newsFetcher` to fetch and format news results.

---

## 5. youtube_groups.go

- **NewYouTubeManager**: Constructor.
- **reviewAndRefreshChannels**: **(b) Business Logic**. Automated channel verification scheduler.
- **CleanupOldData**: **(c) Cache/ORM**. Local cache retention worker.
- **CleanupCache**: **(c) Cache/ORM**. Local cache cleaner.
- **DataRetentionCleanup**: **(c) Cache/ORM**. Local ORM/file storage cleaner.
- **ListGroupsHandler**: **(a) Pure HTTP Wrapper**. Retrieves groups list and writes JSON.
- **CreateGroupHandler**: **(b) Business Logic / (c) Cache/ORM**. Validates fields, updates JSON file storage.
- **DeleteGroupHandler**: **(b) Business Logic / (c) Cache/ORM**. Updates JSON file storage by deleting a group.
- **ManagerStatsHandler**: **(a) Pure HTTP Wrapper**. Calls stats aggregation and writes JSON.
- **aggregateManagerStats**: **(b) Business Logic**. Computes runtime stats (total groups, channels, active tokens).
- **channelHasTokenFile**: **(b) Business Logic**. Audits local filesystem for OAuth tokens.

---

## 6. youtube_channels_v1.go / bulk.go / stats.go

- **ListChannels**: **(a) Pure HTTP Wrapper**. Passes query parameters to database / ORM.
- **GetChannel**: **(a) Pure HTTP Wrapper**. Retrieves a channel from the database.
- **DeleteChannel**: **(a) Pure HTTP Wrapper**. Simple database deletion.
- **UpdateChannel**: **(b) Business Logic**. Performs channel updates, checking constraints and metadata.
- **AutoDetectLanguage**: **(b) Business Logic**. Triggers language detection heuristics.
- **BulkDeleteChannels**: **(b) Business Logic**. Iterates deletions across multiple records.
- **MoveChannelToGroupV1**: **(b) Business Logic**. Updates group associations in bulk.
- **ValidateAllTokens**: **(b) Business Logic**. Verifies OAuth tokens validity across channels.
- **ListUndefinedChannels**: **(a) Pure HTTP Wrapper**. Queries unassigned channels.
- **RefreshChannelsMetadata**: **(b) Business Logic**. Enqueues background metadata refresh tasks.
- **GetChannelAnalytics**: **(b) Business Logic**. Assembles analytics reports.
- **GetChannelGroups**: **(a) Pure HTTP Wrapper**. Fetches groups list.
- **DetectDuplicateChannels**: **(b) Business Logic**. Searches for channel duplicates.
- **ExportChannels**: **(b) Business Logic**. Formats channels for CSV/Excel export.
- **GetChannelStats**: **(b) Business Logic**. Computes stats counts.
- **BatchUpdateLanguage**: **(b) Business Logic**. Updates multiple channels' language fields in a single batch.
