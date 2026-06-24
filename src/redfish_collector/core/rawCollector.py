import yaml
import json
import logging
from re import findall
# import os
from os import path,makedirs
from jinja2 import Template
from jsonpath_ng.ext import parse 
# import aiohttp
import asyncio
from asyncio.exceptions import TimeoutError
from aiohttp import ClientConnectorError, ClientResponseError, ClientTimeout, ClientSession, TCPConnector

# Global cap on simultaneous Redfish requests per scrape. A single shared
# semaphore + shared connection pool (see dataCollector) is what actually bounds
# the load on one BMC; without it the per-fetch_all semaphore multiplied across
# recursion + gather and overran weak BMCs (broken pipe / connection resets).
MAX_CONCURRENT_REQUESTS = 8



def readYAMLTemplate(templateFile, serverAddress):
    # Resolve a single path and use it for BOTH the existence check and the open.
    # path.join ignores the module dir when templateFile is already absolute
    # (the service passes absolute paths), so production behaviour is unchanged;
    # this only fixes the relative-path case where isfile and open disagreed.
    config_file_path = path.join(path.dirname(__file__), templateFile)
    if path.isfile(config_file_path):
        with open(config_file_path, 'r') as f:
            yamlContent = f.read()
            configData = yaml.safe_load(yamlContent)
            logging.debug("Component Schema Data: %s" % (configData))
            return configData
    else:
        logging.error("[%s] Can not find: %s" % (serverAddress,templateFile))
        return

def jsonpathCollector(content,expression,output='value'):
    jsonpath_expr = parse(str(expression))
    if output == 'fullpath&value':
        result = {str(match.full_path): match.value for match in jsonpath_expr.find(content)}
    else:
        result = [match.value for match in jsonpath_expr.find(content)]
        if result == []:
            return False
    return result

def getKeyDictFromURLPath(url, keyDict):
    pathSegment = [segment for segment in url.split("/") if segment]
    dataKeyDict = {key.replace(">>", "", 1): pathSegment[value] for key, value in keyDict.items() if key.startswith(">>")}
    if dataKeyDict == {}:
        return {}
    else:
        return dataKeyDict

def dataJSONWriter(dataRaw,fileDir,fileName, serverAddress):
    if path.isdir(fileDir):
        logging.debug("[%s] Dir %s is existed" % (serverAddress,fileDir))
    else:
        logging.info("[%s] Dir %s is not existed, will create it" % (serverAddress,fileDir))
        makedirs(fileDir, exist_ok=True)
    try:
        with open('%s%s' % (fileDir,fileName), 'w') as file:
            json.dump(dataRaw, file)
        logging.debug("[%s] Write data successfully at %s%s" % (serverAddress,fileDir,fileName))
    except Exception as e:
        logging.error("[%s] There dataJSONWriter error with %s" % (serverAddress,e))
        json.dump([],file)
    return
async def fetch(url, token, session, serverAddress, semaphore: asyncio.Semaphore):
    headers = {'X-Auth-Token': token}
    
    retries = 5
    backoffFactor = 0.5
    
    for attempt in range(1, retries + 1):
        try:
            async with semaphore:
                async with session.get(url, headers=headers, ssl=False) as response:
                    response.raise_for_status()
                    return await response.json()
        except ClientResponseError as e:
            # 4xx (except 429) is permanent for this request: retrying just wastes
            # load on an already-weak BMC and will never succeed. Fail fast.
            if e.status < 500 and e.status != 429:
                logging.error("[%s] Non-retryable HTTP %s for url %s: %s", serverAddress, e.status, url, e)
                return {"status": e.status, "data": None, "success": False, "error_message": f"HTTP {e.status} for URL {url}"}
            transientError = e
        except (ClientConnectorError, TimeoutError, OSError) as e:
            transientError = e
        except Exception as e:
            logging.error("[%s] Unexpected error during fetch: %s", serverAddress, e)
            return {"status": 500, "data": None, "success": False, "error_message": f"Unexpected error: {e} for URL {url}"}

        # transient failure (timeout / connection / 5xx / 429): back off and retry
        logging.debug("[%s] Attempt %s with url: %s failed: %s", serverAddress, attempt, url, transientError)
        if attempt == retries:
            logging.error("[%s] Max retries reached. Giving up.", serverAddress)
            statusCode = getattr(transientError, 'status', None) or getattr(transientError, 'code', 500)
            return {"status": statusCode, "data": None, "success": False, "error_message": f"Request Error: {transientError} for URL {url}"}
        delay = backoffFactor * (2 ** (attempt - 1))
        logging.debug("[%s] Retrying in %.2f seconds...", serverAddress, delay)
        await asyncio.sleep(delay)

async def fetch_all(urls: list, token, serverAddress, session, semaphore):
    # session + semaphore are owned by dataCollector and shared across the whole
    # scrape, so concurrency is globally bounded instead of per-call.
    try:
        tasks = [fetch(url, token, session, serverAddress, semaphore) for url in urls]
        results = await asyncio.gather(*tasks)
        return results
    except Exception as e:
        logging.error("[%s] Fetch all URL error: %s", serverAddress, e)
        raise

async def rawDataCollector(serverAddress,schemaContent,keyDict: dict,token,logLevel,session,semaphore):
    # Logging is configured once at process start (uvicorn logging.yml / __main__);
    # don't call logging.basicConfig() per request — it's a no-op after the first
    # call anyway and mutates global logging state from inside the hot path.
    #
    # NOTE: keyDict is intentionally NOT copied here. The crawl relies on mutating
    # it in place to hand extracted path keys (e.g. `serverid` from
    # /redfish/v1/Systems/<id>, via the `>>serverid` directive in Common.yml) from
    # the bootstrap crawl to the model-component crawls. The concurrency race is
    # instead solved by giving each gathered top-level component its OWN copy at
    # the call site in dataCollector (P0-2) — see `dict(keyIDDict)` there.
    logging.debug("[%s] Key schemaContent: %s" % (serverAddress,schemaContent))
    logging.debug("[%s] Key ID Dict: %s" % (serverAddress,keyDict))
    if isinstance(schemaContent,dict):
        if '$inituri' in schemaContent:
            # dynamicValue = bool(re.search(r"\{\{\s*[\w]+\s*\}\}", schemaContent['$inituri']))
            dynamicValueList = [match.strip() for match in findall(r"\{\{(.*?)\}\}", schemaContent['$inituri'])]
            logging.debug("[%s] Dynamic Value List from Schema: %s" % (serverAddress,dynamicValueList))
            if dynamicValueList == []:
                uri = schemaContent['$inituri']
            else:
                for dynamicValue in dynamicValueList:
                    if dynamicValue not in keyDict:
                        logging.error("[%s] Can't see dynamic value: %s" % (serverAddress,dynamicValue))
                        return []
                uri = Template(schemaContent['$inituri']).render(keyDict)
        else:
            logging.error("[%s] Can't find $inituri field in schema, please check again")
            return []

        url = "https://%s%s" % (serverAddress,uri)
        logging.debug("[%s] Father URL: %s" % (serverAddress,url))
        # dataRaw = dict()
        if '$jsonpath' in schemaContent:
            tempRaw = (await fetch_all([url],token,serverAddress,session,semaphore))[0]
            # dataRaw =dataRaw[0]
            childURIList = jsonpathCollector(tempRaw,str(schemaContent['$jsonpath']))
            # logging.info(childURIList)
            if childURIList is False:
                logging.warning("[%s] Child URI List isn't existed with %s" % (serverAddress, schemaContent))
                logging.debug("[%s] childURIList:\n%s" % (serverAddress,tempRaw))
                return
            else:
                childURLList = ["https://%s%s" % (serverAddress,path) for path in childURIList]
            dataRawList = await fetch_all(childURLList,token,serverAddress,session,semaphore)
            if dataRawList is None:
                logging.error("[%s] Get data failed" % serverAddress)
                return dataRawList
            for key in schemaContent:
                if isinstance(schemaContent[key],dict):
                    logging.debug("[%s] Found child component: %s" % (serverAddress,key))
                    count = 0
                    for childURL, _ in zip(childURLList,dataRawList):
                        childKey = getKeyDictFromURLPath(childURL, schemaContent)
                        updatedKeyDict = keyDict | childKey
                        logging.debug("[%s] Updated key: %s" % (serverAddress,updatedKeyDict))
                        # keyDict.update(getKeyDictFromURLPath(childURL, schemaContent))
                        try:
                            dataRawList[count][key] = await rawDataCollector(serverAddress,schemaContent[key],updatedKeyDict,token,logLevel,session,semaphore)
                            count+=1
                        except Exception as e:
                            logging.error("[%s] There error with %s" % (serverAddress,e))
            for childURL, _ in zip(childURLList,dataRawList):
                keyDict.update(getKeyDictFromURLPath(childURL, schemaContent))
        else:
            dataRawList = await fetch_all([url],token,serverAddress,session,semaphore)
            keyDict.update(getKeyDictFromURLPath(url, schemaContent))
            return dataRawList
        return dataRawList
    else:
        logging.error("[%s] Schema Content isn't dict type, please check again" % serverAddress)
        return []

async def dataCollector(serverAddress,username,password,templateDir,logLevel):
    # Logging is configured once at process start; no per-request basicConfig here.
    ### Read schema from schemas/Common.yml file
    # endpointURL = "https://%s" % serverAddress
    # auth = (username,password)
    base = templateDir + "schemas/Common.yml"
    # base = templateDir + "schemas/HPEProLiantGen10.yml"
    commonSchema=readYAMLTemplate(base, serverAddress)
    if commonSchema is None:
        logging.error("[%s] Can't generate common schema, please check again" % serverAddress)
        return
    # keyIDDict = {'serverAddress': serverAddress}
    keyIDDict = {}
    logging.debug("[%s] Type of commonSchema %s" % (serverAddress,type(commonSchema)))

    # One shared connection pool + semaphore for the WHOLE scrape (token POST,
    # every crawl request and logout). This is the core weak-BMC fix: total
    # concurrent requests to a single BMC are bounded by MAX_CONCURRENT_REQUESTS
    # instead of being multiplied per fetch_all call, and the TCP/TLS connection
    # is reused throughout.
    tokenValue = None
    logoutURL = None
    connector = TCPConnector(limit=MAX_CONCURRENT_REQUESTS, limit_per_host=MAX_CONCURRENT_REQUESTS, ssl=False)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with ClientSession(connector=connector, timeout=ClientTimeout(total=180)) as session:
        for basePoint in commonSchema['Metadata']:
            if "$tokenuri" in commonSchema['Metadata'][basePoint]:
                tokenURL = "https://%s%s" % (serverAddress,commonSchema['Metadata'][basePoint]['$tokenuri'])
                payload = {"UserName": username,"Password": password}
                logging.debug("[%s] token URL %s and Payload: %s" % (serverAddress,tokenURL,payload))
                for attempt in range(1, 4):
                    try:
                        async with session.post(tokenURL,json=payload, ssl=False, timeout=ClientTimeout(total=60)) as response:
                            response.raise_for_status()
                            tokenData = response.headers
                            logging.debug("[%s] Token Data: %s" % (serverAddress,tokenData))
                            if 'X-Auth-Token' in tokenData:
                                tokenValue = tokenData['X-Auth-Token']
                                # Some Redfish BMCs omit (or oddly format) the
                                # Location header; .get() avoids a KeyError. Without
                                # it we still have a valid token, so proceed and just
                                # skip logout (logoutURL stays None).
                                location = tokenData.get('Location')
                                if location:
                                    if location.startswith('https://'):
                                        logoutURL = location
                                    else:
                                        logoutURL = "https://%s%s" % (serverAddress,location)
                                else:
                                    logging.warning("[%s] Token response has no 'Location' header; will skip logout" % serverAddress)
                                logging.info("[%s] Get Token Value successfully" % serverAddress)
                                break
                            else:
                                logging.error("[%s] Can't get X-Auth-Token from response headers" % serverAddress)
                    except ClientResponseError as e:
                        # 401/403/404 etc. = wrong creds / wrong token URI. Retrying
                        # 3× won't help and only loads the BMC — abort immediately.
                        if e.status < 500 and e.status != 429:
                            logging.error("[%s] Auth failed (HTTP %s), not retrying: %s" % (serverAddress, e.status, e))
                            return
                        logging.error("[%s] Transient error when getting token: %s" % (serverAddress,e))
                    except (ClientConnectorError, TimeoutError) as e:
                        logging.error("[%s] There is error when getting token: %s" % (serverAddress,e))

                    if attempt < 3:
                        logging.debug("[%s] Retrying to get token (Attempt %d)" % (serverAddress, attempt + 1))
                        await asyncio.sleep(2 ** (attempt - 1))
                    else:
                        logging.error("[%s] Failed to get token after 3 attempts" % serverAddress)
                        return

            if tokenValue is None:
                logging.error("[%s] No auth token available, aborting collect" % serverAddress)
                return
            commonRaw = await rawDataCollector(serverAddress,commonSchema['Metadata'][basePoint],keyIDDict,tokenValue,logLevel,session,semaphore)
            if not commonRaw:
                # rawDataCollector returns [] / None when it can't read the base
                # Redfish data (e.g. /redfish/v1/Systems on a weak/unreachable BMC).
                # Abort cleanly so the caller emits PhysicalServer_Query=0 instead of
                # crashing on an empty index.
                logging.error("[%s] Could not read base Redfish data (e.g. /redfish/v1/Systems); aborting collect" % serverAddress)
                return
            vendorData = commonRaw[0]

        if 'Manufacturer' in vendorData:
            manufacturer = vendorData['Manufacturer']
            logging.debug("[%s] Manufacturer: %s" % (serverAddress,manufacturer))
        else:
            logging.error("[%s] We can't generate Manufacturer value, Please check JSONPath or else!" % serverAddress)
            return

        if 'Model' in vendorData:
            model = vendorData['Model']
            logging.debug("[%s] Model: %s" % (serverAddress,model))
        else:
            logging.error("[%s] We can't generate Model value, Please check JSONPath or else!" % serverAddress)
            return

        if 'Id' in vendorData:
            vendorId = vendorData['Id']
            logging.debug("[%s] VendorId: %s" % (serverAddress,vendorId))
        else:
            logging.error("[%s] We can't generate VendorId value, Please check JSONPath or else!" % serverAddress)
            return

        modelSchema = None
        for i in commonSchema['ModelSchema']:
            if i in manufacturer:
                logging.info("[%s] This's %s Server - Founded Vendor Name %s" % (serverAddress,i,manufacturer))
                for j in commonSchema['ModelSchema'][i]:
                    if str(j) in str(model):
                        logging.info("[%s] Model using %s - Founded Model Name %s" % (serverAddress,j,model))
                        modelSchema = commonSchema['ModelSchema'][i][j]
                        break
                    else:
                        logging.debug("[%s] Model isn't %s - Founded Model Name %s" % (serverAddress,j,model))
            else:
                logging.debug("[%s] This's not %s Server - Founded Vendor Name %s" % (serverAddress,i,manufacturer))
        if modelSchema:
            logging.info("[%s] We will generate data model with schema file: %s" % (serverAddress,modelSchema))
        else:
            logging.error("[%s] We couldn't find any schema similar with server model: %s. Please check schema directory" % (serverAddress,model))
            return

        # logging.info(vendorData)
        modelSchemaDir = templateDir + "schemas/" + modelSchema
        schema=readYAMLTemplate(modelSchemaDir, serverAddress)
        # logging.info(schema)
        if schema is None or not isinstance(schema, dict) or 'Data' not in schema or 'Metadata' not in schema:
            logging.error("[%s] Can't generate vendor schema, please check again" % serverAddress)
            return
        dataNewSchema = schema['Data']
        # Each top-level component gets its OWN copy of keyIDDict so the
        # concurrently-gathered crawls can't race on a shared dict (P0-2 fix).
        data = [rawDataCollector(serverAddress,schema['Metadata'][component],dict(keyIDDict),tokenValue,logLevel,session,semaphore) for component in schema['Metadata']]
        results = await asyncio.gather(*data)
        dataRaw = dict()
        # Only attempt logout when we actually have a session URI; a missing
        # Location header leaves logoutURL=None (see token acquisition above).
        if logoutURL:
            try:
                async with session.delete(logoutURL, headers={'X-Auth-Token': tokenValue}, ssl=False, timeout=ClientTimeout(total=60)) as response:
                    if response.status == 200 or response.status == 204:
                        logging.info("[%s] Logged out successfully" % serverAddress)
                    else:
                        logging.error("[%s] Logout failed with status code: %s" % (serverAddress,response.status))
            except Exception as e:
                logging.error("[%s] There is error when logout: %s" % (serverAddress,e))
                return
        else:
            logging.warning("[%s] No logout URL (Location missing); skipping logout" % serverAddress)


    for component, result in zip(schema['Metadata'], results):
        dataRaw[component] = result
    logging.debug("[%s] DataRaw: %s" % (serverAddress,dataRaw))
    fileDir = '/tmp/redfish-data/RawData/'
    fileName = '%s.json' % serverAddress
    dataJSONWriter(dataRaw,fileDir,fileName,serverAddress)
    return dataRaw,dataNewSchema,modelSchemaDir

if __name__ == '__main__':
    serverAddress='10.97.99.1'
    username='readonly'
    password='juniper@123'

    # serverAddress='10.97.12.3'
    # username='readonly'
    # password='juniper@123'

    serverAddress='10.97.12.2'
    username='readonly'
    password='juniper@123'

    logLevel='info'
    templateDir='./templates/'

    # Standalone runs need logging configured here since the functions no longer
    # call basicConfig themselves (the service configures it via logging.yml).
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logLevel.upper())
    dataRaw,dataNewSchema,modelSchemaDir = asyncio.run(dataCollector(serverAddress,username,password,templateDir,logLevel=logLevel))
    # logging.info(dataRaw)

    # cleaned_data = dataReconstructor(dataRaw, dataNewSchema, modelSchemaDir)

    # logging.info(cleaned_data)
    # logging.info(newData)
