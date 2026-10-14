package routes

import (
	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/gin-gonic/gin"
)

// RegisterRunningHubRoutes mounts all RunningHub endpoints.
// User-facing routes use JWT auth — the caller's Bearer token is the user's
// login JWT, not an API key. sub2api resolves the user from the JWT and checks
// their credit balance before dispatching to the RH account pool.
// Admin routes remain behind the existing admin middleware.
func RegisterRunningHubRoutes(
	v1 *gin.RouterGroup,
	h *handler.RunningHubHandler,
	jwtAuth middleware.JWTAuthMiddleware,
	adminAuth middleware.AdminAuthMiddleware,
) {
	if h == nil {
		return
	}

	rh := v1.Group("/rh/v1", gin.HandlerFunc(jwtAuth))
	{
		rh.GET("/credits", h.Credits)
		rh.GET("/workflows", h.ListWorkflows)
		rh.POST("/workflows/:app_id", h.Submit)
		rh.GET("/tasks", h.ListTasks)
		rh.GET("/tasks/:id", h.Query)
		rh.POST("/tasks/:id/cancel", h.Cancel)
		rh.POST("/media", h.Upload)
	}

	adminRH := v1.Group("/admin/runninghub", gin.HandlerFunc(adminAuth))
	{
		adminRH.GET("/settings", h.AdminGetSettings)
		adminRH.PUT("/settings", h.AdminUpdateSettings)

		adminRH.GET("/accounts", h.AdminListAccounts)
		adminRH.POST("/accounts", h.AdminAddAccount)
		adminRH.PATCH("/accounts/:id", h.AdminUpdateAccount)
		adminRH.DELETE("/accounts/:id", h.AdminDeleteAccount)

		adminRH.GET("/workflows", h.AdminListWorkflows)
		adminRH.POST("/workflows", h.AdminCreateWorkflow)
		adminRH.GET("/workflows/:id", h.AdminGetWorkflow)
		adminRH.PATCH("/workflows/:id", h.AdminUpdateWorkflow)
		adminRH.POST("/workflows/:id/refresh", h.AdminRefreshWorkflow)
		adminRH.DELETE("/workflows/:id", h.AdminDeleteWorkflow)
		adminRH.POST("/media", h.AdminUploadMedia)
		adminRH.GET("/media/:id/file", h.AdminGetMediaFile)
		adminRH.GET("/rh-file", h.AdminProxyRHFile)

		adminRH.GET("/users/:id/rh-credits", h.AdminGetUserCredits)
		adminRH.POST("/users/:id/rh-credits", h.AdminGrantRH)
	}
}
