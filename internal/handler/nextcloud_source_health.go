package handler

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// Health is an administrator-only, source-scoped database observation. It
// never probes Nextcloud or reports a per-file publication result.
func (h *NextcloudSourcePairingHandler) Health(c *gin.Context) {
	pair, ds, ok := h.authorizedPair(c)
	if !ok {
		return
	}
	if ds == nil || pair.State == "aborted" {
		c.JSON(http.StatusConflict, gin.H{"error": "source_pairing_closed"})
		return
	}
	health, err := h.repo.SourceHealth(c.Request.Context(), pair, time.Now().UTC())
	if err != nil {
		sourcePairingError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"health": health})
}
