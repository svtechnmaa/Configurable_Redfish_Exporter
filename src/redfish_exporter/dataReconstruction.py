import yaml
import json
from jsonpath_ng.ext import parse 
from os import path
from jinja2 import Template
# from yaml.loader import SafeLoader
import requests
# import urllib3
import logging
import re
# from line_profiler import profile

# urllib3.disable_warnings()
requests.packages.urllib3.disable_warnings()
requests.adapters.DEFAULT_RETRIES = 3

def readYAMLTemplate(templateFile, dynamicInput):
    config_file_path = path.join(path.dirname(__file__), templateFile)
    if path.isfile(config_file_path):
        with open(templateFile, 'r') as f:
            yamlContent = f.read()
            renderedContent = Template(yamlContent).render(dynamicInput)
            configData = yaml.safe_load(renderedContent)
            logging.debug("Component Schema Data: %s" % (configData))
            return configData
    else:
        logging.error("Can not find: %s" % (templateFile))
        return

#@profile
def jsonpathCollector(content,expression,output='value'):
    jsonpath_expr = parse(str(expression))
    if output == 'fullpath&value':
        result = {str(match.full_path): match.value for match in jsonpath_expr.find(content)}
    else:
        result = [match.value for match in jsonpath_expr.find(content)]
        if result == []:
            return False
    return result

def fixListConverter(data):
    if isinstance(data, dict):
        if all(isinstance(member, int) for member in data.keys()):
            return [fixListConverter(value) for member, value in sorted(data.items())]
        else:
            return {member: fixListConverter(value) for member, value in data.items()}
    elif isinstance(data, list):
        return [fixListConverter(i) for i in data]
    else:
        return data

#@profile
def dataRawCollector(serverAddress,username,password,schema,url=None):
    logging.debug("Reading Schema: %s" % schema)
    if '$url' in schema:
            url = schema['$url']
    if url is None:
        logging.error("Can't find URL")
        return
    else:
        logging.debug("use url from input: %s" % url)

    parentDataRaw = requests.get(url, verify=False, auth=(username, password)).json()
    # parentDataRaw = session.get(url).json()
    logging.debug("Parent Data Raw: %s" % parentDataRaw)

    if '$chain' in schema:
        if '$jsonpath' in schema['$chain']:
            chainURIs = jsonpathCollector(parentDataRaw,schema['$chain']['$jsonpath'])
            if not chainURIs:
                logging.error("May be $jsonpath for $chain level was wrong")
                return
            with requests.Session() as session:
                session.auth = (username, password)
                session.verify = False
                session.keep_alive = False
                # session.timeout=60
                for i in chainURIs:
                    chainURL = 'https://%s%s' % (serverAddress,i)
                    # currentDataRaw = requests.get(chainURL, verify=False, auth=(username, password)).json()
                    currentDataRaw = session.get(chainURL).json()
        else:
            logging.error("If you define $chain, you need define $jsonpath in $chain level")
            return
    else:
        currentDataRaw = parentDataRaw

    if '$members' in schema:
        if '$jsonpath' in schema['$members']:
            currentSchema = schema['$members']
            logging.debug("schema jsonpath: %s" % currentSchema['$jsonpath'])
            memberURIs = jsonpathCollector(currentDataRaw,currentSchema['$jsonpath'])
            logging.debug("memberURIs: %s" % memberURIs)

            if not memberURIs:
                logging.error("May be $jsonpath was wrong")
                return
            else:
                memberDataRaw = list()
                childName = list()
                for i in currentSchema:
                    if '$' in str(i):
                        continue
                    else:
                        logging.info("Find %s" % i)
                        childName.append(i)
    
                for memberURI in memberURIs:
                    memberURL = 'https://%s%s' % (serverAddress,memberURI)
                    logging.info("memberURL %s" % memberURL)
                    
                    with requests.Session() as session:
                        session.auth = (username, password)
                        session.verify = False
                        session.keep_alive =False
                        tempRaw = session.get(memberURL).json()
                        # tempRaw = requests.get(memberURL, verify=False, auth=(username, password)).json()

                    for child in childName:
                        if isinstance(currentSchema[child],dict):
                            tempRaw[child] = dataRawCollector(serverAddress,username,password,currentSchema[child],url=memberURL)
                            logging.debug(tempRaw[child])
                        else:
                            logging.error("Child is not correct")
                    memberDataRaw.append(tempRaw)
            return memberDataRaw
        else:
            logging.error("If you define $members, you need define $jsonpath in $members level")
            return
    return parentDataRaw

#@profile
def dataReconstruction(serverAddress,username,password,templateDir,logLevel):
    logFormat = '%(asctime)s [%(levelname)s] [' + serverAddress + '] %(message)s'  
    logging.basicConfig(format=logFormat, level=logLevel.upper())
    base = templateDir + "schemas/baseInfo.yml"
    baseURL=readYAMLTemplate(base, {'serverAddress': serverAddress})
    # vendorURI= None

    session = requests.Session()
    session.auth = (username, password)
    session.verify = False
    session.keep_alive = False
    # session.timeout = 60

    for linkList in baseURL['Base']:
        logging.debug(baseURL['Base'][linkList])
        baseSystemURL=baseURL['Base'][linkList]['URL']
        logging.debug(baseSystemURL)
        try:
            # getVendor = requests.get(baseSystemURL, verify=False, auth=(username, password))
            getVendor = session.get(baseSystemURL)
            if getVendor.status_code == 401:
                logging.warning("Status code %s returned, check your username/password is correct or has correct privileges to execute via Redfish API with URL %s" % (getVendor.status_code, baseSystemURL))
            if getVendor.status_code != 200:
                logging.warning("GET command failed to check supported version, status code %s returned with URL %s" % (getVendor.status_code, baseSystemURL))
            logging.debug("str: %s" % (baseURL['Base'][linkList]['VendorURI']))
            logging.debug("getVendor: %s" % (getVendor.json()))
            tempValue = jsonpathCollector(getVendor.json(),str(baseURL['Base'][linkList]['VendorURI']))
            tempValue = tempValue[0]
            if tempValue:
                vendorKey = tempValue
                logging.info("vendorKey: %s" % (vendorKey))
                correctLink = linkList
            else:
                logging.error("Can't find vendorKey, please check vendorKey in baseInfo.yml file"% (serverAddress))
                return
        except Exception as err:
            logging.warning("Error with URL %s: %s" % (baseSystemURL,err))

    informationURL = 'https://%s%s' % (serverAddress,vendorKey)
    logging.debug("Base URL: %s" % (informationURL))
    # getInformation = requests.get(informationURL, verify=False, auth=(username, password))
    getInformation = session.get(informationURL)
    session.close()
    logging.debug("Information Raw Data: %s" % (getInformation.json()))
    commonDict = dict()
    for commonInfo in baseURL['Base'][correctLink]:
        logging.debug("Common Information: %s" % (commonInfo))
        if commonInfo == 'URL' or commonInfo == 'VendorURI':
            continue
        else:
            tempList = jsonpathCollector(getInformation.json(),str(baseURL['Base'][correctLink][commonInfo]))
            commonDict[commonInfo] = tempList[0]
            logging.debug("%s: %s" % (commonInfo,commonDict[commonInfo]))
    if 'Manufacturer' in commonDict:
        manufacturer = commonDict['Manufacturer']
        logging.info("Manufacturer: %s" % (manufacturer))
    else:
        logging.error("We can't generate Manufacturer value, Please check JSONPath or else!")
        return
    
    if 'Model' in commonDict:
        model = commonDict['Model']
        logging.info("Model: %s" % (model))
    else:
        logging.error("We can't generate Model value, Please check JSONPath or else!")
        return

    if 'VendorId' in commonDict:
        vendorId = commonDict['VendorId']
        logging.info("VendorId: %s" % (vendorId))
    else:
        logging.error("We can't generate VendorId value, Please check JSONPath or else!")
        return

    modelSchema = None
    for i in baseURL['Model']:
        if i in manufacturer:
            logging.info("This's %s Server - Founded Vendor Name %s" % (i,manufacturer))
            for j in baseURL['Model'][i]:
                if j in model:
                    logging.info("Model using %s - Founded Model Name %s" % (j,model))
                    modelSchema = baseURL['Model'][i][j]
                else:
                    logging.debug("Model isn't %s - Founded Model Name %s" % (j,model))        
            break
        else:
            logging.debug("This's not %s Server - Founded Vendor Name %s" % (i,manufacturer))
    if modelSchema:
        logging.info("We will generate data model with schema file: %s" % (modelSchema))
    else:
        logging.error("We couldn't find any schema similar with server model: %s. Please check schema directory" % (model))
        return

    modelSchemaDir = templateDir + "schemas/" + modelSchema
    schema=readYAMLTemplate(modelSchemaDir, {'serverAddress': serverAddress ,'vendorURI': vendorId})
    # logging.info("Schema: %s" % schema)
    metadata = schema['Metadata']
    dataNewSchema = schema['Data']

    dataRaw = dict()
    for componentName in metadata:
        logging.debug("Working with %s" % (componentName))
        dataRaw[componentName] = dataRawCollector(serverAddress,username,password,metadata[componentName])
    # return dataRaw,dataNewSchema
    logging.debug("dataRaw: %s" % dataRaw)
    IdPoints = dict()
    idList = jsonpathCollector(dataNewSchema,str("$..Id"),output='fullpath&value')
    # logging.info("Id: %s" % idList)
    for key in idList:
        result = jsonpathCollector(dataRaw,idList[key],output='fullpath&value')
        # logging.info("Id from data raw %s: %s" % (key,result))
        IdPoints.update(result)
    logging.debug("Id Points: %s" % IdPoints)
    
    dataTemplate = dict()

    elementFlag = None
    for abspath in IdPoints:
        current = dataTemplate
        elements = re.split(r'\.|\[|\]', abspath)
        elements = [int(k) if k.isdigit() else k for k in elements if k != '']
        # logging.info(elements)
        schemaCurrent = dataNewSchema
        for element in elements[:-1]:
            # logging.info("Current: %s " % dataTemplate)
            current = current.setdefault(element, dict())
            if not isinstance(element,int):
                schemaCurrent = schemaCurrent[element]
        # logging.info(IdPoints[abspath])
        newDict = dict()
        newDict['Id'] = IdPoints[abspath]
        
        if elementFlag != elements[0]:
            rootElementDataRaw = {elements[0]: dataRaw[elements[0]]}
            elementFlag = elements[0]
        # logging.info("Data raw via element:\n%s" % rootElementDataRaw)
        for elementKey in schemaCurrent:
            logging.debug("Current Key: %s" % elementKey)
            if elementKey == 'Id':
                continue
            if not isinstance(schemaCurrent[elementKey],dict):
                newJSONPath = re.sub(elements[-1], schemaCurrent[elementKey], abspath)
                logging.debug("newJSONPath: %s" % newJSONPath)
                result = jsonpathCollector(rootElementDataRaw,newJSONPath)
                logging.debug("Result: %s" % result)
                if result is not False:
                    if elementKey == 'Status':
                        if isinstance(result[0],dict):
                            if 'State' not in result[0]:
                                result[0].update({'State':'Unknown'})
                            if 'Health' not in result[0]:
                                result[0].update({'Health':'Unknown'})
                        elif isinstance(result[0],str):
                            stateTemp = result[0]
                            result[0] = dict()
                            result[0].update({'State': stateTemp,'Health':'Unknown'})
                        elif result[0] is None:
                            result[0] = dict()
                            result[0].update({'State': 'Unknown','Health':'Unknown'})
                        else:
                            logging.error("It's a bug for Status define: %s" % result[0])
                    newDict.update({elementKey: result[0]})
                    # return newDict
                else:
                    if elementKey == 'Status':
                        newDict.update({'Status': {'State': 'Unknown','Health':'Unknown'}})
                    else:
                        newDict.update({elementKey:'Unknown'})
        current = current.update(newDict)
    newData = fixListConverter(dataTemplate)
    with open('/tmp/%s_rawdata.txt' % serverAddress, 'w') as file:
        json.dump(dataRaw, file)
    with open('/tmp/%s_newdata.txt' % serverAddress, 'w') as file:
        json.dump(newData, file)
    logging.info("Generate data Raw successfully")
    return dataRaw, newData

if __name__ == '__main__':
    # serverAddress='10.97.12.3'
    # username='juniper'
    # password='juniper@123'

    serverAddress='10.98.11.12'
    username='juniper'
    password='juniper@123'

    logLevel='info'
    templateDir='./templates/'

    dataRaw,newData = dataReconstruction(serverAddress,username,password,templateDir,logLevel=logLevel)
    # logging.info(dataRaw)
    # logging.info(newData)

