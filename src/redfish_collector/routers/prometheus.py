from fastapi import HTTPException,APIRouter, Query
from fastapi.responses import PlainTextResponse
from pydantic import IPvAnyAddress
from starlette import status
from ..core.dataReconstruction import dataReconstructor
from ..core.rawCollector import dataCollector,jsonpathCollector,readYAMLTemplate
from os import path
import re
import json
import logging
import time
import yaml
from prometheus_client import generate_latest, Gauge, CollectorRegistry

REDFISH_DATA = '/tmp/redfish-data/'

# settings = Settings()
templateDir = config_path = path.join(path.dirname(__file__), '../core/templates/')

# REGISTRY.unregister(PROCESS_COLLECTOR)
# REGISTRY.unregister(PLATFORM_COLLECTOR)
# REGISTRY.unregister(REGISTRY._names_to_collectors['python_gc_objects_collected_total'])
# registry = REGISTRY
# REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')
CACHE={}

router = APIRouter(
    prefix='/metrics',
    tags=['Prometheus Metrics']
)

@router.get("", status_code=status.HTTP_200_OK)
async def read_all(serverAddress: IPvAnyAddress = Query(None), config: str = Query(None), loglevel: str = Query("info")) -> PlainTextResponse:
    componentMetrics={}
    if (serverAddress is None) or (config is None):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail = 'Collect metrics Failed, please add params')
    else: 
        registry = CollectorRegistry()
        componentMetrics['PhysicalServer_Query'] = Gauge('PhysicalServer_Query','physical server query status',['serverAddress'],registry=registry)
        timeCalled = time.ctime()
        serverAddress = str(serverAddress)
        try:
            metricsConfigFile = f'{templateDir}configs/{config}.yml'
            metricsConfig = readYAMLTemplate(metricsConfigFile)
            if "Auth" not in metricsConfig:
                logging.error(f"[{serverAddress}] Can't find Auth in config file {config}")
                raise ValueError("Auth details missing in configuration file.")
            username = metricsConfig['Auth']['Username']
            password = metricsConfig['Auth']['Password']

            cacheInfo= f"{serverAddress}{config}"
            if cacheInfo in CACHE:
                cachedResponse, timestamp = CACHE[cacheInfo]
                if time.time() - timestamp < 180:
                    logging.info(f"[{serverAddress}] Serving health-check from cache.")
                    return PlainTextResponse(cachedResponse)
                
            inventoryFile = f'{REDFISH_DATA}inventory.yml'
            block = {'serverAddress': str(serverAddress), 'username': str(username), 'password': str(password) ,'timeCalled': timeCalled}
            if path.isfile(inventoryFile):
                with open(inventoryFile, 'r') as f:
                    inventory = yaml.safe_load(f) or []
                if inventory == []:
                    with open(inventoryFile, 'w') as f:
                        yaml.dump([block], f, default_flow_style=False)
                else:
                    existedServer = False
                    for server in inventory:
                        if server['serverAddress'] == str(serverAddress):
                            server['username'] = str(username)
                            server['password'] = str(password)
                            server['timeCalled'] = timeCalled
                            with open(inventoryFile, 'w') as f:
                                yaml.dump(inventory, f, default_flow_style=False)
                            existedServer = True
                            break
                    if existedServer is False:
                        inventory.append(block)
                        with open(inventoryFile, 'w') as f:
                            yaml.dump(inventory, f, default_flow_style=False)
            else:
                with open(inventoryFile, 'w') as f:
                    yaml.dump([block], f, default_flow_style=False)
        except Exception as err:
            logging.error("Generate instance failed: %s" %err)
            return False

    try:
        dataRaw, dataNewSchema, modelSchemaDir = await dataCollector(serverAddress,username,password,templateDir,loglevel)
        dataReconstructor(dataRaw, dataNewSchema, modelSchemaDir, serverAddress,loglevel)

        dataDir = f'{REDFISH_DATA}NewData/{serverAddress}.json'
        with open(dataDir, 'r') as file:
            collectedData = json.load(file)

        hostName = collectedData['Common'][0]['HostName']
        # componentMetrics={}
        for metric in metricsConfig['Metrics']:
            standard = ['Name', 'Description', 'Label', 'Datapoint', 'Result', 'Type']
            errorFlag = 0
            for key in standard:
                if key not in metric:
                    logging.error("[%s] Can't find %s in metrics key, please check again!" % (serverAddress, key))
                    errorFlag = 1
            if errorFlag == 1:
                continue
            else:
                if metric['Type'] == 'Gauge':
                    componentMetrics[metric['Name']] = Gauge(metric['Name'],metric['Description'],metric['Label'],registry=registry)
                    elements = metric['Datapoint'].split('.')
                    logging.debug("[%s] Split datapoint %s to %s" % (serverAddress,metric['Datapoint'],elements))

                    if not isinstance(collectedData.get(elements[0]), list):
                        logging.warning(f"[{serverAddress}] Data point root is not a list: {elements[0]}")
                        continue
                    # logging.error("Fist Point: %s" % firstPointData)
                    idList = jsonpathCollector(collectedData,str("$..Id"),output='fullpath&value')
                    for memberID in idList:
                        if elements[-1] in memberID and elements[0] in memberID:
                            labelList = list()
                            for label in metric['Label']:
                                if label == 'ServerAddress':
                                    labelList.append(serverAddress)
                                elif label == 'HostName':
                                    labelList.append(hostName)
                                else:
                                    newJSONPath = re.sub('Id', label, memberID)
                                    result = jsonpathCollector(collectedData,newJSONPath)
                                    if result is not False:
                                        labelList.append(result[0])
                                    else:
                                        labelList.append('Unknown')
                                        continue
                            logging.debug("[%s] List Label: %s" % (serverAddress,labelList))
                            if 'State' in metric['Result'] or 'Health' in metric['Result']:
                                if 'StatusCode' not in metric:
                                    logging.error("[%s] Can't find StatusCode, please check again!" % (serverAddress))
                                    continue
                                state = 'Status.' + metric['Result']
                                newJSONPath = re.sub('Id', state, memberID)
                                logging.debug("[%s] newJSONPath: %s" % (serverAddress,newJSONPath))
                                value = jsonpathCollector(collectedData,str(newJSONPath))
                                if value is False:
                                    logging.error("[%s] Value for %s isn't existed: %s" % (serverAddress,str(newJSONPath),value))
                                    codeNumber = 999
                                elif value is None:
                                    logging.warning("[%s] Value for %s is None: %s" % (serverAddress,str(newJSONPath),value))
                                    codeNumber = 99
                                elif value[0] is None:
                                    logging.warning("[%s] Value[0] for %s is None: %s" % (serverAddress,str(newJSONPath),value[0]))
                                    codeNumber = 99
                                else:
                                    if value[0].upper() in metric['StatusCode']:
                                        codeNumber = metric['StatusCode'][value[0].upper()]
                                        logging.debug("[%s] Value and CodeNumber: %s and %s" % (serverAddress,value,codeNumber))
                                    else:
                                        logging.error("[%s] Maybe value isn't correct at %s: %s" % (serverAddress, metric['Name'],value))
                                        codeNumber = 999
                                componentMetrics[metric['Name']].labels(*labelList).set(float(codeNumber))
                            else:
                                newJSONPath = re.sub('Id', metric['Result'], memberID)
                                value = jsonpathCollector(collectedData,str(newJSONPath))
                                if value is False:
                                    componentMetrics[metric['Name']].labels(*labelList).set(999)
                                else:
                                    value =value[0]
                                logging.debug("Value type: %s" % type(value))
                                if isinstance(value,int) or isinstance(value,float):
                                    componentMetrics[metric['Name']].labels(*labelList).set(float(value))       
                                else:
                                    logging.error("[%s] Value %s isn't float: %s" % (serverAddress,metric['Result'],value))
                                    componentMetrics[metric['Name']].labels(*labelList).set(999)   
                            logging.debug("[%s] ID List Collected with in tree %s to %s" % (serverAddress,metric['Datapoint'],labelList)) 
                else:
                    logging.error("[%s] Not found Type %s, please call Admin" % (serverAddress, metric['Type']))

        componentMetrics['PhysicalServer_Query'].labels(str(serverAddress)).set(1)
        metrics = generate_latest(registry)
        # REQUEST_TIME.observe(time.time() - start_time)
        CACHE[cacheInfo] = (metrics, time.time())
        return PlainTextResponse(metrics)

    except Exception as err:
        if 'PhysicalServer_Query' in componentMetrics:
            componentMetrics['PhysicalServer_Query'].labels(str(serverAddress)).set(0)
            metrics = generate_latest(registry)
            # REQUEST_TIME.observe(time.time() - start_time)
            logging.error(f"[{serverAddress}] Metric collection failed: {err}", exc_info=True)
            rawPath = f'{REDFISH_DATA}RawData/{serverAddress}.json'
            newPath = f'{REDFISH_DATA}NewData/{serverAddress}.json'
            try:
                with open(rawPath, 'w'): pass
                with open(newPath, 'w'): pass
                return PlainTextResponse(metrics)
            except IOError as e:
                logging.error(f"[{serverAddress}] Failed to truncate files: {e}")
        else:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f'Collect metrics Failed: {err}')
