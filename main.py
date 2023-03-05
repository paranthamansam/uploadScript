import os
import pandas
import s3util
import boto3
import dynamodbutil

# session = boto3.Session(
#     aws_access_key_id=os.environ["accessKey"],
#     aws_secret_access_key=os.environ["secretKey"]
# )

bucket_name = "bimpro-test"
membership_id ="mem:535"
rfa_dir = "./"

#prepare s3 key {revit_version}/{category}/{name.rfa}
def prepare_key(row): 
    return "{}/{}/{}".format(row["revit_version"],row["category"],row["family_file_name"])

# check the category is exists or need to create and return the category
def create_get_category(categoryName):
    try:
        category_list = dynamodbutil.GetUtil("cat")
        category = list(filter(lambda item : item["name"] == categoryName , category_list));
        if (category and len(category) > 0):
            return category[0];
        else:
            category = dynamodbutil.CreateUtils(categoryName, "cat")
        pass
    except Exception as e:
        print(str(e))
        raise e

# check the manufacturer is exists or need to create and return the manufaturer
def create_get_manufacture(manufacture):
    try:
        manufacturer_list = dynamodbutil.GetUtil("mnf")
        manufacturer = list(filter(lambda item: item["name"] in manufacture , manufacturer_list));
        if (manufacturer and len(manufacturer) > 0):
            return manufacturer[0];
        else:
            manufacturer = dynamodbutil.CreateUtils(manufacture, "mnf")
        pass
    except Exception as e:
        print(str(e))
        raise e
    
def get_prepare_family(membership_id,row):
    try:
        #try get all families
        familes = dynamodbutil.GetFamilies(membership_id, str(row['revit_version']))
        if (familes and len(familes) > 0) :
            #check already exists
            family = list(filter(lambda item : item['display-name'] == row['display_name'], familes))
            if (family and len(family) > 0):
                rec = list(family)[0]
                rec['version'] = row['version']
                return rec
        # no match found
        return None
    except Exception as e:
        print(str(e))
        raise e

# Check category or manufacturer is exists on table if yes then use or create new
def create_family(row):
    try:
        family = {}
        category = create_get_category(row["category"])
        manufactuere = create_get_manufacture(row["manufacturer"])
        memberships = dynamodbutil.GetUtil(membership_id)


        family = get_prepare_family(memberships[0]['guid'], row)
        if (not family):
            family = {
                "license" : memberships[0]['guid'],
                "name" : row['family_file_name'],
                "revit-version" : str(row['revit_version']),
                "display-name": row['display_name'],
                "version" : str(row['version']),
                "category-guid" :category['guid'],
                "manufacture-guid": manufactuere['guid']
            }   
        
        dynamodbutil.CreateFamilies(family)
    except Exception as e:
        print(str(e))
        raise e

# process the csv, upload file to s3, write corresponding entry to the dynamodb.
def process():
    try:
        # read file directory
        dir_list = os.listdir(rfa_dir)
        # read csv
        csvFile = pandas.read_csv("Fabber_Updated Family.csv")
        print("{} records found ".format(len(csvFile)))
        for index, row in csvFile.iterrows():
            if(any(item in row["family_file_name"] for item in dir_list)):
                file_name = row["family_file_name"]
                # Upload file to s3
                s3util.upload_file(file_name, bucket_name, prepare_key(row))
                create_family(row)
                print("File No. {} completed".format(str(index + 1)))
            else:
                print("file not found")

        print("{} records successfully uploaded".format(len(csvFile)))
    except:
        print("An exception occurred")

if(__name__=="__main__"):
    process()
# process()


