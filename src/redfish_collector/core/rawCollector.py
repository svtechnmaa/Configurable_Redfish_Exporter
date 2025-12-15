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
from aiohttp import ClientConnectorError, ClientResponseError, ClientTimeout, ClientSession



def readYAMLTemplate(templateFile):
    config_file_path = path.join(path.dirname(__file__), templateFile)
    if path.isfile(config_file_path):
        with open(templateFile, 'r') as f:
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
    
    async with semaphore:
        for attempt in range(1, retries + 1):
            try:
                async with session.get(url, headers=headers, ssl=False) as response:
                    response.raise_for_status() 
                    return await response.json()
            except (ClientConnectorError, ClientResponseError, TimeoutError, OSError) as e:
                logging.debug("[%s] Attempt %s with url: %s failed: %s", serverAddress, attempt, url, e)
                
                if attempt == retries:
                    logging.error("[%s] Max retries reached. Giving up.", serverAddress)
                    statusCode = getattr(e, 'status', None) or getattr(e, 'code', 500)
                    return {"status": statusCode, "data": None, "success": False, "error_message": f"Request Error: {e} for URL {url}"}
                else:
                    delay = backoffFactor * (2 ** (attempt - 1))
                    logging.debug("[%s] Retrying in %.2f seconds...", serverAddress, delay)
                    await asyncio.sleep(delay)
            except Exception as e:
                logging.error("[%s] Unexpected error during fetch: %s", serverAddress, e)
                return {"status": 500, "data": None, "success": False, "error_message": f"Unexpected error: {e} for URL {url}"}

async def fetch_all(urls: list, token, serverAddress):
    timeout = ClientTimeout(total=180)
    semaphore = asyncio.Semaphore(8)
    try:
        async with ClientSession(timeout=timeout) as session:
            tasks = [fetch(url, token, session, serverAddress,semaphore) for url in urls]
            results = await asyncio.gather(*tasks)
            return results
    except Exception as e:
        logging.error("[%s] Fetch all URL error: %s", serverAddress, e)
        raise

async def rawDataCollector(serverAddress,schemaContent,keyDict: dict,token,logLevel):
    logFormat = '%(asctime)s [%(levelname)s] %(message)s'
    logging.basicConfig(format=logFormat, level=logLevel.upper())
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
            tempRaw = (await fetch_all([url],token,serverAddress))[0]
            # dataRaw =dataRaw[0]
            childURIList = jsonpathCollector(tempRaw,str(schemaContent['$jsonpath']))
            # logging.info(childURIList)
            if childURIList is False:
                logging.warning("[%s] Child URI List isn't existed with %s" % (serverAddress, schemaContent))
                logging.debug("[%s] childURIList:\n%s" % (serverAddress,tempRaw))
                return
            else:
                childURLList = ["https://%s%s" % (serverAddress,path) for path in childURIList]
            dataRawList = await fetch_all(childURLList,token,serverAddress)
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
                            dataRawList[count][key] = await rawDataCollector(serverAddress,schemaContent[key],updatedKeyDict,token,logLevel)
                            count+=1
                        except Exception as e:
                            logging.error("[%s] There error with %s" % (serverAddress,e))
            for childURL, _ in zip(childURLList,dataRawList):
                keyDict.update(getKeyDictFromURLPath(childURL, schemaContent))
        else:
            dataRawList = await fetch_all([url],token,serverAddress)
            keyDict.update(getKeyDictFromURLPath(url, schemaContent))
            return dataRawList
        return dataRawList
    else:
        logging.error("[%s] Schema Content isn't dict type, please check again" % serverAddress)
        return []

async def dataCollector(serverAddress,username,password,templateDir,logLevel):
    # logging.getLogger().handlers[0].flush()
    logFormat = '%(asctime)s [%(levelname)s] %(message)s'  
    logging.basicConfig(format=logFormat, level=logLevel.upper())

    ### Read schema from schemas/Common.yml file
    # endpointURL = "https://%s" % serverAddress
    # auth = (username,password)
    base = templateDir + "schemas/Common.yml"
    # base = templateDir + "schemas/HPEProLiantGen10.yml"
    commonSchema=readYAMLTemplate(base)
    if commonSchema is None:
        logging.error("[%s] Can't generate common schema, please check again" % serverAddress)
        return
    # keyIDDict = {'serverAddress': serverAddress}
    keyIDDict = {}
    logging.debug("[%s] Type of commonSchema %s" % (serverAddress,type(commonSchema)))

    for basePoint in commonSchema['Metadata']:
        if "$tokenuri" in commonSchema['Metadata'][basePoint]:
            tokenURL = "https://%s%s" % (serverAddress,commonSchema['Metadata'][basePoint]['$tokenuri'])
            timeout = ClientTimeout(total=60)
            payload = {"UserName": username,"Password": password}
            logging.debug("[%s] token URL %s and Payload: %s" % (serverAddress,tokenURL,payload))
            try:
                async with ClientSession(timeout=timeout) as session:
                    async with session.post(tokenURL,json=payload, ssl=False) as response:
                        tokenData = response.headers
                        logging.debug("[%s] Token Data: %s" % (serverAddress,tokenData))
                        if 'X-Auth-Token' in tokenData:
                            tokenValue = tokenData['X-Auth-Token']
                            if tokenData['Location'].startswith('https://'):
                                logoutURL = tokenData['Location']
                            else:
                                logoutURL = "https://%s%s" % (serverAddress,tokenData['Location'])
                            logging.info("[%s] Get Token Value successfully" % serverAddress)
                            pass
                        else:
                            logging.error("[%s] Can't get X-Auth-Token from response headers" % serverAddress)
                            return
                    pass
            except Exception as e:
                logging.error("[%s] There is error when getting token: %s" % (serverAddress,e))
                return
        # vendorData = await fetch_all(childURIList,token)
        vendorData = (await rawDataCollector(serverAddress,commonSchema['Metadata'][basePoint],keyIDDict,tokenValue,logLevel))[0]

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
                if j in model:
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
    schema=readYAMLTemplate(modelSchemaDir)
    # logging.info(schema)
    dataNewSchema = schema['Data']
    if schema is None:
        logging.error("[%s] Can't generate vendor schema, please check again" % serverAddress)
        return
    data = [rawDataCollector(serverAddress,schema['Metadata'][component],keyIDDict,tokenValue,logLevel) for component in schema['Metadata']]
    results = await asyncio.gather(*data)
    dataRaw = dict()
    try:
        timeout = ClientTimeout(total=60)
        async with ClientSession(timeout=timeout) as session:
            async with session.delete(logoutURL, headers={'X-Auth-Token': tokenValue}, ssl=False) as response:
                if response.status == 200 or response.status == 204:
                    logging.info("[%s] Logged out successfully" % serverAddress)
                    pass
                else:
                    logging.error("[%s] Logout failed with status code: %s" % (serverAddress,response.status))
            pass
    except Exception as e:
        logging.error("[%s] There is error when logout: %s" % (serverAddress,e))
        return
    
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

    dataRaw,dataNewSchema,modelSchemaDir = asyncio.run(dataCollector(serverAddress,username,password,templateDir,logLevel=logLevel))
    # logging.info(dataRaw)

    # cleaned_data = dataReconstructor(dataRaw, dataNewSchema, modelSchemaDir)

    # logging.info(cleaned_data)
    # logging.info(newData)