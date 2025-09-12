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
                for (String instanceType : instanceTypes) {
                    if (instanceType.startsWith("#")) continue;
                    try {
                    double[] usageAndEmbodiedEnergy = client.getEnergyAndEmbodiedEmissionsEstimates(Provider.AWS, instanceType.trim());
                    writer.write(instanceType + ", " + usageAndEmbodiedEnergy[0]+ ", " + usageAndEmbodiedEnergy[1]);
                    writer.newLine();
                    } catch (Exception e) {
                        System.err.println("Exception caught for instance type "+instanceType);
                    }
                }
            }
        } else if (args.length == 1){
            double[] usageAndEmbodiedEnergy = client.getEnergyAndEmbodiedEmissionsEstimates(Provider.AWS,args[0]);
            System.out.println("Usage KWh: " + usageAndEmbodiedEnergy[0]);
            System.out.println("Embodied emissions gCO2eq: " + usageAndEmbodiedEnergy[1]);
        }
    }
}
