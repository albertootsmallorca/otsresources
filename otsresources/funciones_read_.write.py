from azure.storage.blob import BlobServiceClient
from pyspark.sql.types import *
from pyspark.sql.functions import *

#Flatten array of structs and structs 
def flatten(df): 
    # compute Complex Fields (Lists and Structs) in Schema    
    complex_fields = dict([(field.name, field.dataType) 
                                for field in df.schema.fields 
                                if type(field.dataType) == ArrayType or  type(field.dataType) == StructType]) 
    while len(complex_fields)!=0: 
        col_name=list(complex_fields.keys())[0] 
        print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name]))) 
        # if StructType then convert all sub element to columns. 
        # i.e. flatten structs 
        if (type(complex_fields[col_name]) == StructType): 
            #expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]] 
            expanded = [col(col_name+'.'+k).alias(k) for k in [ n.name for n in  complex_fields[col_name]]] 
            df=df.select("*", *expanded).drop(col_name) 
        # if ArrayType then add the Array Elements as Rows using the explode function 
        # i.e. explode Arrays 
        elif (type(complex_fields[col_name]) == ArrayType):     
            df=df.withColumn(col_name, explode_outer(col_name)) 
        # recompute remaining Complex Fields in Schema        
        complex_fields = dict([(field.name, field.dataType) 
                                for field in df.schema.fields 
                                if type(field.dataType) == ArrayType or  type(field.dataType) == StructType]) 
    return df

def save_csv(df, filename, blob_container='raw/configuracion', sep = ';', header = True, index = False, encoding = 'utf-8-sig'):
    blob_service_client = BlobServiceClient.from_connection_string(f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_access_key};EndpointSuffix=core.windows.net")
    
    res = df.to_csv(sep = sep, header = header, index = index, encoding = encoding)
    blob_client = blob_service_client.get_blob_client(container=blob_container, blob=filename)
    blob_client.upload_blob(data=res, overwrite = True)
    print('Guardado correctamente ' + filename)

def read_csv_from_blob(filename, header=True, sep=';', blob_container='raw'): 
    """
    Returns pandas DataFrame from blobstorage
    prev: spark.conf.set has to be launched
    """
    filePath1 = "wasbs://" + blob_container + "@" + storage_account_name + f".blob.core.windows.net/configuracion/{filename}"
#     schema = StructType([StructField("Válida", IntegerType(), True), StructField("Bayes", IntegerType(), True)])
#     df= spark.read.schema(schema).format("csv").load(filePath1, header = True, sep = sep)
    df= spark.read.format("csv").load(filePath1, header = True, sep = sep)
    
    return df