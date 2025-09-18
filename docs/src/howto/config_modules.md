# Modules configuration

The enrichment modules are configured in a file called `default-config.json`. This file is included in the JAR and looks like this:

```json
{{#include ../../../src/main/resources/default-config.json}}
```

This determines which modules are used and in what order but also configures their behaviour. For instance, the default coefficient set for the ccf.Networking module is _0.001_ kWh/Gb.

## Change the configuration

In order to use a different configuration, for instance to replace a module with an other one, or change their configuration (like the network coefficient above), 
you simply need to write a json file with your changes and pass it as an argument to the Spark job with '-c'.


