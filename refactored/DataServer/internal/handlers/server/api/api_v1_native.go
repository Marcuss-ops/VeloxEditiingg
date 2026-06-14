package api

import (
	"github.com/gin-gonic/gin"
	"velox-server/internal/handlers/remote/livestream"
	"velox-server/internal/handlers/remote/submission"
	"velox-server/internal/integrations/youtube"
	"velox-server/internal/queue"
)

// RegisterV1NativeRoutes registers minimal GO-native v1 API endpoints so the
// frontend can run without Python Job Master (port 8002).
func RegisterV1NativeRoutes(r *gin.Engine, sq *queue.StreamsQueue, dataDir string, ytService *youtube.Service) {
	// Initialize livestream handlers
	livestreamHandlers := livestream.NewLivestreamHandlers(ytService, dataDir)
	// Initialize submission handlers if queue is available
	var submissionHandlers *submission.SubmissionHandlers
	if sq != nil {
		submissionHandlers = submission.NewSubmissionHandlers(sq)
	}

	v1 := r.Group("/api/v1")
	{
		// Submissions (multi-clip job creation)
		if submissionHandlers != nil {
			v1.POST("/submissions", submissionHandlers.CreateSubmission)
			v1.GET("/submissions", submissionHandlers.ListSubmissions)
			v1.GET("/submissions/:id", submissionHandlers.GetSubmission)
			v1.PUT("/submissions/:id", submissionHandlers.UpdateSubmission)
			v1.DELETE("/submissions/:id", submissionHandlers.DeleteSubmission)
			v1.POST("/submissions/:id/cancel", submissionHandlers.CancelSubmission)
			v1.POST("/submissions/:id/retry", submissionHandlers.RetrySubmission)
		}

		// Jobs: GET /jobs and GET/DELETE/POST /jobs/:id are registered in RegisterV1Routes (jobAPI).

		// Workers, Analytics, Channels, Groups, YouTube, drive-links, Ansible: all in RegisterV1Routes.

		// Livestream API
		v1.GET("/livestream", livestreamHandlers.ListStreams)
		v1.POST("/livestream", livestreamHandlers.CreateStream)
		v1.GET("/livestream/status", livestreamHandlers.GetStatus)
		v1.GET("/livestream/:id", livestreamHandlers.GetStream)
		v1.PUT("/livestream/:id", livestreamHandlers.UpdateStream)
		v1.DELETE("/livestream/:id", livestreamHandlers.DeleteStream)
		v1.POST("/livestream/:id/testing", livestreamHandlers.StartTesting)
		v1.POST("/livestream/:id/live", livestreamHandlers.GoLive)
		v1.POST("/livestream/:id/complete", livestreamHandlers.EndStream)
	}
}
