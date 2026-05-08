# Modules configuration

The enrichment modules are configured in a per-provider JSON file bundled in the JAR.
The file used at runtime is selected from the cloud provider (`-p / --provider` CLI
flag, defaulting to `AWS`):

| Provider flag | Resource file              |
|---------------|----------------------------|
| `AWS`         | `default-config-aws.json`  |
| `GOOGLE`      | `default-config-google.json` *(when available)* |
| `AZURE`       | `default-config-azure.json` *(when available)* |

The AWS default looks like this:

```json
{{#include ../../../src/main/resources/default-config-aws.json}}
```

This determines which modules are used and in what order but also configures their behaviour. For instance, the Networking module uses different coefficients for intra-region, inter-region, and external data transfers, all configurable via the `network_coefficients_kwh_gb` map.

## Change the configuration

In order to use a different configuration, for instance to replace a module with another one, or change their configuration (like the network coefficient above),
you simply need to write a json file with your changes and pass it as an argument to the Spark job with `-c`. A custom config passed via `-c` overrides the per-provider default.

`-p` still applies when you pass `-c`: provider-aware modules (such as `Water` or
`AverageCarbonIntensity`) need it to pick the correct region-keyed lookups. If your
custom config targets Azure, pass `-p AZURE` alongside `-c` so those modules don't
fall back to the AWS default.

## Selecting a provider

If you do not pass `-c`, SPRUCE picks the bundled config matching the provider:

```shell
spark-submit --class com.digitalpebble.spruce.SparkJob ./target/spruce-*.jar \
  -i ./curs -o ./output -p AWS
```

`-p` defaults to `AWS`, so existing AWS workflows do not need to change.
