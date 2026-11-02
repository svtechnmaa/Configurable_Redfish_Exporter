# Configurable Redfish Exporter

The End goal is a Prometheus exporter that can automatically create server metric base on Redfish API and JSON Metric mapping configuration.
This is a Next Generation that include two main functions:
- Redfish Collector: We collect all devices that defined in inventory and reconstruct model.
- Prometheus Exporter: We use new model from Redfish Collector to generate prometheus metrics, response them when prometheus server call.

Folder structure should as follow:
- source code
- configuration file for mapping logic from individual vendor to common YAML/JSON schema
- configuration file for mapping logic from common YAML/JSON schema to exporter metric
- deployment files

## Guide:
#### Add Readonly ILO/IDRAC Account (IMPORTANT)
You need to define a readonly account on ILO/IDRAC, We will use this account for collecting data (on sample.yml file or somewhere like that):
* Example:

  ```
  cat sample.yml

  Auth:
  Username: CHANGE_ME_USERNAME
  Password: CHANGE_ME_PASSWORD
  ...
  ```
Tips: If somecases we don't have an unique account for all ILO/IDRAC, we can define more file with: <name>.yml same with sample.yml that I defined, then we can add other account and use prometheus query with parameter: config: <name>. Check `Pull metrics from Redfish Exporter` or `Query metrics for testing` section

#### Create Exporter
There're two ways that you can use Redfish-Exporter:
* Using Container Images:
  - Pull Redfish Exporter images via `docker pull` command (if you're using docker):

    ``` bash
    docker pull ghcr.io/svtechnmaa/redfish-exporter:<version>
    ```

    Run `docker run` command if you use Docker:

    ``` bash
    docker run -d --name <your exporter name> -p 9814:9814 redfish-exporter:<version>
    ```

* Runing with source:
  - You should install python library that included in requirement.txt
  - Just run `Uvicorn` in source directory:
    
    ``` bash
    uvicorn redfish_collector.main:app --host="0.0.0.0" --port=9814 --log-config="redfish_collector/logging/logging.yml"
    ```

#### Config Structure

* Structure:

    ```
    templates/
    |-- configs
    |   |-- inventory.yml                            ### Define devices that need for collecting, you can check example
    |   |-- sample.yml                               ### Define Auth (Using Readonly ILO/IDRAC Account) - Metrics (Using to define prometheus metrics)
    |-- schemas
        |-- Common.yml                               ### Schema that get common information for all vendor server (of course with Redfish DMTF supporting)
        |-- DellPowerEdgeR630.yml                    ### Schema that get vendor information
        |-- DellPowerEdgeR650.yml                    ### ...       
        |-- DellPowerEdgeR750.yml                    ### ...
        |-- HPEProLiantGen10.yml
        |-- HPEProLiantGen9.yml
    ```

#### Pull metrics from Redfish Exporter
You need to have Prometheus Server or Victoria Metrics or else, change configuration on config file:
* For example:
  
    ```
    global:
      scrape_interval: 2m 
      evaluation_interval: 30s
    scrape_configs:
    - job_name: "prometheus"
      static_configs:
      - targets: ["localhost:9090"]
    - job_name: 'node_proxmox'
      scrape_interval: 3m
      scrape_timeout: 2m
      params:
        serverAddress: ['<server-idrac-or-ilo-ip>']
        config: ['sample']
      static_configs:
      - targets: ["<exporter-ip>:9814"]
    ```

If you want to scale out deployment for load balancing, you should have a `pvc` or `hostpath` that shared data with each pod, like this:

    ```
    volumeMounts:
    - mountPath: /opt/redfish_exporter/templates/configs
      name: exporter-config-volume
    - mountPath: /tmp/redfish-data/NewData
      name: redfish-data-volume
      subPath: NewData
    - mountPath: /tmp/redfish-data/RawData
      name: redfish-data-volume
      subPath: RawData

    ---

    volumes:
    - name: redfish-data-volume
      persistentVolumeClaim:
        claimName: redfish-data-pvc
    - name: exporter-config-volume
      configMap:
        defaultMode: 420
        items:
        - key: sample.yml
          path: sample.yml
        name: redfish-exporter-configmap
    ```
NOTE: Just mount NewData/RawData/sample. Don't mount inventory in redfish-data folder when scale out.

#### Query metrics for testing
You can use `curl` command to query server for testing data:
* For example:

    ```bash
    curl -XGET 'http://<exporter-ip>:<exporter-port>/metrics?serverAddress=<server-ip>&config=<file name in config dir>'
    ```

Query parameters of `/metrics`:

| Parameter | Required | Description |
|-----------|----------|-------------|
| `serverAddress` | yes | IP address of the iLO/iDRAC (BMC) |
| `config` | yes | Config file name in the config dir, without `.yml` (e.g. `sample`) |
| `loglevel` | no | One of `debug`, `info` (default), `warning`, `error`. Any other value is ignored and treated as `info`. See `Debug a scrape` below |

Other way you can connect to FastAPI doc WebUI:
* For example:

    ```bash
    http://<exporter-ip>:<exporter-port>/docs
    ```

#### Debug a scrape
Data collected from vendors uses different models, so the exporter converts it to the models defined in the schema.
To inspect the converted (normalized) data for one target, enable debug artifacts and scrape with `loglevel=debug`.

1. Enable debug artifacts in the `Tuning` block of **every** config file in the config dir (e.g. `sample.yml`, `sample2.yml`, ...). They are disabled by default.
   `Tuning.Debug.*` keys are process-wide: one debug store serves all configs, so the exporter refuses to start if any of them differ between config files (`FATAL: profile validation failed at startup: Process-wide Tuning.Debug.WriteNormalizedData mismatch between profiles 'sample' and 'sample2'`). A key left out counts as its default, so a file without a `Debug` block conflicts with one that sets `WriteNormalizedData: true`.

    ```yaml
    Auth:
      Username: CHANGE_ME_USERNAME
      Password: CHANGE_ME_PASSWORD
    Tuning:
      Debug:
        WriteNormalizedData: true        # must be a YAML boolean, not "true"
        # RootDirectory: /tmp/redfish-data   # default, must be an absolute path
        # MaxFileBytes: 5242880              # default 5 MiB per file
        # MaxFilesPerTarget: 4
        # MaxTotalBytes: 104857600           # default 100 MiB in total
        # RetentionSeconds: 3600             # files older than this are purged
    Metrics:
      ...
    ```

    Restart the exporter after changing the config files.

2. Scrape the target with `loglevel=debug`:

    ```bash
    curl -s 'http://<exporter-ip>:9814/metrics?serverAddress=<server-ip>&config=sample&loglevel=debug'

    # or, letting curl encode the parameters:
    curl -s -G 'http://<exporter-ip>:9814/metrics' \
      --data-urlencode 'serverAddress=<server-ip>' \
      --data-urlencode 'config=sample' \
      --data-urlencode 'loglevel=debug'
    ```

3. Read the artifact. It is written to `<RootDirectory>/<sha256 of "<server-ip>\0<config>">/NormalizedData.json`. The directory name is a hash, never the raw IP:

    ```bash
    docker exec exporter /bin/sh -c 'ls /tmp/redfish-data/*/NormalizedData.json'

    # compute the directory name for one target/config pair
    DIR=$(printf '%s\0%s' '<server-ip>' 'sample' | sha256sum | cut -d' ' -f1)
    docker exec exporter /bin/sh -c "cat /tmp/redfish-data/$DIR/NormalizedData.json"
    ```

Notes:
- `loglevel` applies only to that request. It does **not** change the exporter's log verbosity. Logging is configured once at startup from `redfish_collector/logging/logging.yml`. To see `DEBUG` log lines, change the level there and restart.
- Without `WriteNormalizedData: true`, `loglevel=debug` writes nothing. Without `loglevel=debug`, scrapes write nothing even when debug artifacts are enabled.
- A failure to write the debug artifact is logged and never fails the scrape.
