from __future__ import print_function
from boto3 import client as boto3_client
import base64
import redis
import os
import uuid
import json
import time
from datetime import datetime
from elasticsearch import Elasticsearch

elasticache_endpoint = "cv19redis-001.d9jy7a.0001.euw1.cache.amazonaws.com"
redis_mapping = redis.StrictRedis(host=elasticache_endpoint, db=3, charset="utf-8", decode_responses=True)

host = 'https://search-covid198-es-2-x6zr2th7oiq7sjzp653cs3k3xm.eu-west-1.es.amazonaws.com/'
region = 'eu-west-1'
es = Elasticsearch(
    hosts=host,
)
es.ping()

def create_connection(sensor_id: str) -> str:
     patient_id = None
     
     search_param ={
         'size':1,
         '_source': ['patient_Id'],
         'query': {
             'match': {
                 'sensors_list.unit_Id': sensor_id
            }
        }
     }
     res = es.search(index="patients_v1", body=search_param)['hits']['hits']
     
     if(res != []):
         patient_id = res[0]['_source']['patient_Id']
         redis_mapping.set(sensor_id, patient_id)   
         
     return patient_id 
                    
    
def get_patient_id(sensor_id: str) -> str:
    patient_id = None
    patient_id = redis_mapping.get(sensor_id)
    return patient_id


def recreate_mapping() -> bool:
    status = True 
    try:
        lambda_client = boto3_client('lambda')
        msg = {"recreating":"redis mapping", "at": datetime.now()}
        lambda_client.invoke(FunctionName='Recreating-mapping_table',
                             InvocationType='Event',
                             Payload=json.dumps(msg))
    except:
        status = False
    return status


def is_db_failed(event) -> bool:
    res = es.search(index='sensors_v1', size=1)['hits']['hits']
    if(res != []):
        sample_unitId = res[0]['_source']['unit_id']
        
    # This sample sensor unit id has to appear in the redis, if no - it means it failed.
    if(redis_mapping.exists(sample_unitId) != 1):
        return True
    return False
    
    
def is_redis_available() -> bool:
    r = redis.StrictRedis(host=elasticache_endpoint, port=6379)
    try:
        r.ping()
    except:
        return False
    return True


def lambda_handler(event, context):
    output = []
    is_redis_avail = is_redis_available()      
    
    # If the db failed, recreate mapping table.
    is_mapping_exists = True
    if(is_db_failed(event) == True):
        is_mapping_exists = recreate_mapping()
        
    # Redis connection
    r = redis.StrictRedis(host=elasticache_endpoint, port=6379, db=0,charset="utf-8", decode_responses=True)
    
    for record in event['records']:
        # Decoding, because we get the date encoded to base64.
        payload = base64.b64decode(record['data'])
        dt = json.loads(payload)
        
        if(is_redis_avail == True and is_mapping_exists == True):
            try:
                # Hoping there's a connection already.
                patient_id = get_patient_id(dt['unitId'])
                
                # If no, create the connection.
                if(patient_id == None):
                    patient_id = create_connection(dt['unitId'])
                    
                # If this patient is connected to any sensor.
                if(patient_id != None):
                    # Retrieving the record belongs to the current patient id, from redis.
                    current_known = r.hget('LastKnown', patient_id)
                    current_update = r.hget('last_update', patient_id)
                
                    # If this patient doesn't recorded in the redis already.
                    if(current_known == None):
                        r.hset('LastKnown', patient_id, json.dumps({'patientId': patient_id, 'age': dt['age'], 'primery_priority': {},'secondery_priority': {}}))
                        current_known = r.hget('LastKnown', patient_id)
                    if(current_update == None):
                        r.hset('last_update', patient_id , json.dumps({'patientId': patient_id, 'updates': {}}))
                        current_update = r.hget('last_update', patient_id) 
                
                    # Convert the records to json.
                    current_known = json.loads(current_known)
                    current_update = json.loads(current_update)
            
                    uniqe_id = str(uuid.UUID(bytes=os.urandom(16), version=4))
                    current_known['Id'] = uniqe_id
                    current_update['Id'] = uniqe_id
                
                    primery_priority = dt['primery_priority']
                    secondery_priority = dt['secondery_priority']
                        
                    # Nano seconds since epoch. this timestamp is used only in LastKnown & last_update. also - calc and ui use the same format when they retrieve this data.
                    ns_epoch = int(time.time_ns() // 1000000)

                    # Update exist measures - both values and timestamp.
                    current_known['timeTag'] = ns_epoch
                    if (dt['age'] != current_known['age']):
                        current_known['age'] = dt['age']
                    for key, val in primery_priority.items():
                        current_known['primery_priority'][key] = val
                        current_update['updates'][key] = ns_epoch
                    for key, val in secondery_priority.items():
                        current_known['secondery_priority'][key] = val
                        current_update['updates'][key] = ns_epoch
                      
                    r.hset('LastKnown', patient_id, json.dumps(current_known))
                    r.hset('last_update', patient_id, json.dumps(current_update))
            except:
                pass

        # Anyway, transfer the data back to kinesis in order it to be written to ES.
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(payload)        
        }
        output.append(output_record)  
        
    return {'records': output}
