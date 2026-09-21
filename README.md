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

Other way you can connect to FastAPI doc WebUI:
* For example:

    ```bash
    http://<exporter-ip>:<exporter-port>/docs
    ```

#### Compare raw data with new data
As you know data collected from vendors are different models, so we need convert them to models that defined in schema.
For troubleshooting, you can go into exporter container with `/bin/sh` and check in `/tmp` directory:
* For example:

    ```bash
    docker exec exporter /bin/sh -c 'ls /tmp/refish-data/RawData/'
    
    docker exec exporter /bin/sh -c 'ls /tmp/refish-data/NewData/'

    10.97.12.1.json    10.97.12.3.json    10.97.99.1.json
    10.97.12.1.json    10.97.12.3.json    10.97.99.1.json

    docker exec exporter /bin/sh -c 'cat /tmp/refish-data/RawData/10.97.99.1.json'

    docker exec exporter /bin/sh -c 'cat /tmp/refish-data/NewData/10.97.99.1.json'
    ```
