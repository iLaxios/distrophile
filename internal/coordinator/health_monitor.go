package coordinator

import (
	"context"
	"time"

	rd "github.com/iLaxios/distrophile/internal/redis"
	"github.com/iLaxios/distrophile/proto"
	"go.uber.org/zap"
)

// StartHealthMonitor starts a background goroutine that monitors node health
func StartHealthMonitor(ctx context.Context, c *Coordinator) {
	// Use half the heartbeat interval for checking, default to 30 seconds
	intervalSec := c.Cfg.HeartbeatIntervalSec / 2
	if intervalSec <= 0 {
		intervalSec = 30
	}
	checkInterval := time.Duration(intervalSec) * time.Second

	c.Log.Infof("Starting health monitor: check_interval=%v", checkInterval)

	go func() {
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				c.Log.Info("Health monitor stopped")
				return
			case <-ticker.C:
				checkNodeHealth(ctx, c)
			}
		}
	}()
}

// checkNodeHealth checks all nodes and updates their health status
func checkNodeHealth(ctx context.Context, c *Coordinator) {
	// Get all nodes from MongoDB
	nodes, err := c.NodeRepo.ListNodes(ctx)
	if err != nil {
		c.Log.Error("Health monitor: failed to list nodes", zap.Error(err))
		return
	}

	if c.Redis == nil {
		return
	}

	for _, node := range nodes {
		// Check if node has active heartbeat in Redis
		isAlive, err := rd.IsNodeAlive(ctx, c.Redis, node.NodeId)
		if err != nil {
			c.Log.Warn("Health monitor: failed to check node heartbeat",
				zap.String("node_id", node.NodeId),
				zap.Error(err))
			continue
		}

		currentState := node.State
		var newState proto.NodeState

		if isAlive {
			// Node is sending heartbeats
			if currentState != proto.NodeState_HEALTHY {
				newState = proto.NodeState_HEALTHY
				c.Log.Infof("Health monitor: Node recovered - %s (%s -> HEALTHY)",
					node.NodeId, currentState.String())
			}
		} else {
			// Node is not sending heartbeats
			if currentState == proto.NodeState_HEALTHY {
				newState = proto.NodeState_UNHEALTHY
				c.Log.Warnf("Health monitor: Node became unhealthy - %s (HEALTHY -> UNHEALTHY)",
					node.NodeId)
			} else if currentState == proto.NodeState_UNHEALTHY {
				// Check if node has been unhealthy for too long
				// For now, we'll keep it as UNHEALTHY
				// In future, could add logic to mark as OFFLINE after extended period
				continue
			}
		}

		// Update state if changed
		if newState != proto.NodeState_UNKNOWN && newState != currentState {
			if err := c.NodeRepo.UpdateNodeState(ctx, node.NodeId, newState); err != nil {
				c.Log.Error("Health monitor: failed to update node state",
					zap.String("node_id", node.NodeId),
					zap.String("new_state", newState.String()),
					zap.Error(err))
			}
		}
	}
}
