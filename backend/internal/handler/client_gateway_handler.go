package handler

import (
	"encoding/json"
	"io"
	"mime"
	"net/http"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func parseClientGatewayBody(c *gin.Context, out any) bool {
	mediaType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || mediaType != "application/json" {
		response.BadRequest(c, "Expected application/json")
		return false
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16*1024)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(out); err != nil {
		response.BadRequest(c, "Invalid model authorization request")
		return false
	}
	if decoder.Decode(new(any)) != io.EOF {
		response.BadRequest(c, "Invalid model authorization request")
		return false
	}
	return true
}

func (h *ClientConfigurationHandler) CreateGatewaySession(c *gin.Context) {
	s, ok := h.verifiedSession(c)
	if !ok {
		return
	}
	var req service.ClientGatewaySessionRequest
	if !parseClientGatewayBody(c, &req) {
		return
	}
	credential, err := h.gateway.Issue(c.Request.Context(), s, req)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, credential)
}

func (h *ClientConfigurationHandler) RevokeGatewaySession(c *gin.Context) {
	s, ok := h.verifiedSession(c)
	if !ok {
		return
	}
	var req service.ClientGatewayRevokeRequest
	if !parseClientGatewayBody(c, &req) {
		return
	}
	result, err := h.gateway.Revoke(c.Request.Context(), s, req)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}
