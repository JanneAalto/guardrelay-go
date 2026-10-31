package main

import (
	"context"
	"fmt"
	"strconv"
)

func (r *smartPiRelay) publishEnergy(ctx context.Context, client mqttPublisher) error {
	// Serialize snapshots with their publications, without holding the sample lock
	// during network operations or blocking disconnect's baseline invalidation.
	r.energyPublishMu.Lock()
	defer r.energyPublishMu.Unlock()
	r.mu.Lock()
	values := [4]float64{r.energy[0], r.energy[1], r.energy[2], r.energy[0] + r.energy[1] + r.energy[2]}
	r.mu.Unlock()
	for i, value := range values {
		key := "energy_total"
		if i < 3 {
			key = fmt.Sprintf("energy_l%d", i+1)
		}
		if err := publish(ctx, client, r.cfg.MQTTBaseTopic+"/state/"+key,
			[]byte(strconv.FormatFloat(value, 'f', -1, 64)), true); err != nil {
			return err
		}
	}
	r.mu.Lock()
	r.energyReady = true
	r.mu.Unlock()
	return nil
}
