// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.carbonara.modules.boavizta;

import com.digitalpebble.carbonara.Provider;

public class APITester {

    public static void main(String[] args) throws Exception {
        BoaviztAPIClient client = new BoaviztAPIClient("http://localhost:5000");

        double[] usageAndEmbodiedEnergy = client.getEnergyEstimates(Provider.AWS,"a1.4xlarge");
        System.out.println("Usage KWh: " + usageAndEmbodiedEnergy[0]);
        System.out.println("Embodied KWh: " + usageAndEmbodiedEnergy[1]);
    }
}
