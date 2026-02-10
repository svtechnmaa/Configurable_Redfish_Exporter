# import json
import logging
import re
import asyncio
from .rawCollector import jsonpathCollector,dataJSONWriter

def remove_odata_elements(d,notUseValue):
    if isinstance(d, dict):
        # Use dictionary comprehension to create a new dict without @odata keys
        return {k: remove_odata_elements(v,notUseValue) for k, v in d.items() if not k.startswith(notUseValue)}
    elif isinstance(d, list):
        # If the element is a list, apply the function to each item in the list
        return [remove_odata_elements(i,notUseValue) for i in d]
    return d  # Return the item if it's neither a dict nor a list

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

def dataReconstructor(dataRaw,dataNewSchema, templateDir, serverAddress,logLevel):
    # logFormat = '%(asctime)s [%(levelname)s] %(message)s'
    # logging.basicConfig(format=logFormat, level=logLevel.upper())
    cleaned_data=dataRaw
    notUseValueList = ['@odata']
    for notUseValue in notUseValueList:
        cleaned_data = remove_odata_elements(cleaned_data,notUseValue)
    # return cleaned_data

    IdPoints = dict()
    idList = jsonpathCollector(dataNewSchema,str("$..Id"),output='fullpath&value')
    # logging.info("[%s] Id: %s" % idList)
    for key in idList:
        result = jsonpathCollector(cleaned_data,idList[key],output='fullpath&value')
        # logging.info("[%s] Id from data raw %s: %s" % (key,result))
        IdPoints.update(result)
    logging.debug("[%s] Id Points: %s" % (serverAddress,IdPoints))
    
    dataTemplate = dict()

    elementFlag = None
    for abspath in IdPoints:
        current = dataTemplate
        elements = re.split(r'\.|\[|\]', abspath)
        elements = [int(k) if k.isdigit() else k for k in elements if k != '']
        # logging.info(elements)
        schemaCurrent = dataNewSchema
        for element in elements[:-1]:
            # logging.info("[%s] Current: %s " % dataTemplate)
            current = current.setdefault(element, dict())
            if not isinstance(element,int):
                schemaCurrent = schemaCurrent[element]
        # logging.info(IdPoints[abspath])
        newDict = dict()
        newDict['Id'] = IdPoints[abspath]
        
        if elementFlag != elements[0]:
            rootElementDataRaw = {elements[0]: cleaned_data[elements[0]]}
            elementFlag = elements[0]
        logging.debug("[%s] Data raw via element:\n%s" % (serverAddress,rootElementDataRaw))
        for elementKey in schemaCurrent:
            logging.debug("[%s] Current Key: %s" % (serverAddress,elementKey))
            if elementKey == 'Id':
                continue
            if not isinstance(schemaCurrent[elementKey],dict):
                newJSONPath = re.sub(elements[-1], schemaCurrent[elementKey], abspath)
                logging.debug("[%s] newJSONPath: %s" % (serverAddress,newJSONPath))
                result = jsonpathCollector(rootElementDataRaw,newJSONPath)
                logging.debug("[%s] Result: %s" % (serverAddress,result))
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
                            logging.error("It's a bug for Status define: %s" % (serverAddress,result[0]))
                    newDict.update({elementKey: result[0]})
                    # return newDict
                else:
                    if elementKey == 'Status':
                        newDict.update({'Status': {'State': 'Unknown','Health':'Unknown'}})
                    else:
                        newDict.update({elementKey:'Unknown'})
        current = current.update(newDict)
    newData = fixListConverter(dataTemplate)
    # with open('/tmp/%s_newdata.txt' % serverAddress, 'w') as file:
    #     json.dump(newData, file)
    fileDir = '/tmp/redfish-data/NewData/'
    fileName = '%s.json' % serverAddress
    dataJSONWriter(newData,fileDir,fileName,serverAddress)
    logging.info("[%s] Generate data Raw successfully" % serverAddress)
    return newData

if __name__ == '__main__':
    serverAddress='10.97.99.1'
    username='readonly'
    password='juniper@123'

    # serverAddress='10.97.12.3'
    # username='readonly'
    # password='juniper@123'

    # serverAddress='10.97.12.2'
    # username='readonly'
    # password='juniper@123'

    logLevel='info'
    templateDir='./templates/'

    # main(serverAddress,username,password,templateDir,logLevel)
    dataRaw,dataNewSchema,modelSchemaDir = asyncio.run(dataCollector(serverAddress,username,password,templateDir,logLevel=logLevel))
    # logging.info(dataRaw)

    cleaned_data = dataReconstructor(dataRaw, dataNewSchema, modelSchemaDir)

    # logging.info(cleaned_data)
    # logging.info(newData)