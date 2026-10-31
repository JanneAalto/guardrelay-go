package main

import (
	"context"
	"strconv"
)

func (r *smartPiRelay) publishEnergy(ctx context.Context, client mqttPublisher) error {
	// Serialize snapshots with their publications, without holding the sample lock
	// during network operations or blocking disconnect's baseline invalidation.
	r.energyPublishMu.Lock()
	defer r.energyPublishMu.Unlock()
	r.mu.Lock()
	values := [8]float64{
		r.energy[0], r.energy[1], r.energy[2], r.energy[0] + r.energy[1] + r.energy[2],
		r.exportedEnergy[0], r.exportedEnergy[1], r.exportedEnergy[2],
		r.exportedEnergy[0] + r.exportedEnergy[1] + r.exportedEnergy[2],
	}
	r.mu.Unlock()
	keys := [...]string{
		"energy_l1", "energy_l2", "energy_l3", "energy_total",
		"energy_export_l1", "energy_export_l2", "energy_export_l3", "energy_export_total",
	}
	for i, value := range values {
		if err := publish(ctx, client, r.cfg.MQTTBaseTopic+"/state/"+keys[i],
			[]byte(strconv.FormatFloat(value, 'f', -1, 64)), true); err != nil {
			return err
		}
	}
	r.mu.Lock()
	r.energyReady = true
	r.mu.Unlock()
	return nil
}
