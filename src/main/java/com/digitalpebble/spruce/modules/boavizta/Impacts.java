// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta;

/** Per-instance impact estimates returned by Boavizta lookups. */
public class Impacts {
    private final double finalEnergyKWh;
    private final double embeddedEmissionsGramsCO2eq;
    private final double abioticDepletionPotentialGramsSbeq;

    public Impacts(double finalEnergyKWh, double embeddedEmissionsGramsCO2eq, double abioticDepletionPotentialGramsSbeq) {
        this.finalEnergyKWh = finalEnergyKWh;
        this.embeddedEmissionsGramsCO2eq = embeddedEmissionsGramsCO2eq;
        this.abioticDepletionPotentialGramsSbeq = abioticDepletionPotentialGramsSbeq;
    }

    public double getFinalEnergyKWh() {
        return finalEnergyKWh;
    }

    public double getEmbeddedEmissionsGramsCO2eq() {
        return embeddedEmissionsGramsCO2eq;
    }

    public double getAbioticDepletionPotentialGramsSbeq() {
        return abioticDepletionPotentialGramsSbeq;
    }
}
