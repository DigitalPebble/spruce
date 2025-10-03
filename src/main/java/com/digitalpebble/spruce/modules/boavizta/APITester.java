// SPDX-License-Identifier: Apache-2.0

package com.digitalpebble.spruce.modules.boavizta;

import com.digitalpebble.spruce.Provider;

import java.util.List;

// if 2 arguments are specified, use the first one as an input file containing
// a list of ec2 instances and the second file as an output containing the instance types
// as well as the energy estimates.
// if a single argument is present, treat it as the instance type to query for
public class APITester {

    public static void main(String[] args) throws Exception {

        BoaviztAPIClient client = new BoaviztAPIClient("http://localhost:5000");

        if (args.length == 2) {
            List<String> instanceTypes = java.nio.file.Files.readAllLines(java.nio.file.Paths.get(args[0]));
            try (java.io.BufferedWriter writer = java.nio.file.Files.newBufferedWriter(java.nio.file.Paths.get(args[1]))) {
                writer.write("# instance_type, electricity_consumption_kwh, embodied_emissions_gco2eq, embodied_adp_gsbeq");
                writer.newLine();
                for (String instanceType : instanceTypes) {
                    if (instanceType.startsWith("#")) continue;
                    try {
                        Impacts usageAndEmbodiedEnergy = client.getImpacts(Provider.AWS, instanceType.trim());
                        writer.write(instanceType + ", " + usageAndEmbodiedEnergy.getFinalEnergyKWh() + ", " + usageAndEmbodiedEnergy.getEmbeddedEmissionsGramsCO2eq() + ", " + usageAndEmbodiedEnergy.getAbioticDepletionPotentialGramsSbeq());
                        writer.newLine();
                    } catch (Exception e) {
                        System.err.println("Exception caught for instance type " + instanceType);
                    }
                }
            }
        } else if (args.length == 1) {
            Impacts usageAndEmbodiedEnergy = client.getImpacts(Provider.AWS, args[0]);
            System.out.println("Usage KWh: " + usageAndEmbodiedEnergy.getFinalEnergyKWh());
            System.out.println("Embodied emissions gCO2eq: " + usageAndEmbodiedEnergy.getEmbeddedEmissionsGramsCO2eq());
            System.out.println("Abiotic depletion potential gSbeq: " + usageAndEmbodiedEnergy.getAbioticDepletionPotentialGramsSbeq());
        }
    }
}
