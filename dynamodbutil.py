import uuid
import boto3
from boto3.dynamodb.conditions import Key

util_table = "sbx_bimpro-util"
family_table = "sbx_bimpro-family"

db = boto3.resource("dynamodb", region_name="us-east-1")
client =  boto3.client("dynamodb", region_name="us-east-1")

def GetUtil(type):
    try:
        table = db.Table(util_table)
        response = None
        response = table.query(
            KeyConditionExpression=Key("bim-type").eq(type)
        )
        
        if len(response["Items"]) == 0:
            return None 

        return response["Items"]

    except Exception as e:
        raise e
    
def CreateUtils(name, type):
    try:
        item = {
            "type" : type,
            "name" : name,
            "guid" : "{}:{}".format(type,str(uuid.uuid4()))
        }
        inc = {
            "guid" : {"S": item["guid"]},
            "bim-type": {"S": item["type"]},
            "name" : {"S": item["name"]}
        }			
        response = client.put_item(TableName=util_table, Item = inc)
        if(response and response["ResponseMetadata"]["HTTPStatusCode"] == 200):
            return item
        else:
            raise Exception("Error while insert record into dynamodb , resoponse: {}".format(str(response)))
    except Exception as e:
        raise e
    
def CreateFamilies(familie):
    try:

        guid = familie['guid'] if ('guid' in familie) else "{}:{}:{}".format("fml",familie["revit-version"],str(uuid.uuid4()))
        item = {
            "guid": {"S": guid},
            "bim-type": {"S": "fml"},
            "name": {"S": familie["name"]},
            "display-name": {"S" : familie["display-name"]},
            "license": {"S": familie["license"]}, "version": {"S": familie["version"]},
            "category-guid": {"S": familie["category-guid"]},
            "manufacture-guid" : {"S": familie["manufacture-guid"]},
            "revit-version" : { "S" : familie["revit-version"]}
        }
        response = client.put_item(TableName=family_table, Item = item)
        if(response and response["ResponseMetadata"]["HTTPStatusCode"] == 200):
            return item
        else:
            raise Exception("Error while insert record into dynamodb , resoponse: {}".format(str(response)))
            
    except Exception as e:
        raise e
    
def GetFamilies(membershipGuid, revitVersion):
    try:
        lastEvaluatedKey= None
        result = []
        while True:
            table = db.Table(family_table)
            response = None
            if lastEvaluatedKey is not None:
                response = table.query(
                    KeyConditionExpression=Key("license").eq(membershipGuid) & Key("guid").begins_with("fml:"+revitVersion),
                    ExclusiveStartKey=lastEvaluatedKey	
                )
            else:
                response = table.query(
                    KeyConditionExpression=Key("license").eq(membershipGuid) & Key("guid").begins_with("fml:"+revitVersion)
                )

                for item in response["Items"]:
                    result.append(item)
            
            if "LastEvaluatedKey" not in response:
                return result
            else:
                lastEvaluatedKey = response["LastEvaluatedKey"]
    except Exception as e:
        print(str(e))
        raise e	