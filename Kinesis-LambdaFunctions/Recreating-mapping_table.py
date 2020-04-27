from elasticsearch import Elasticsearch
import redis

host = 'https://search-covid198-es-2-x6zr2th7oiq7sjzp653cs3k3xm.eu-west-1.es.amazonaws.com/'
region = 'eu-west-1'
es = Elasticsearch(
    hosts=host,
)
es.ping()
    
elasticache_endpoint = "cv19redis-001.d9jy7a.0001.euw1.cache.amazonaws.com"
redis_mapping = redis.StrictRedis(host=elasticache_endpoint, db=3, charset="utf-8", decode_responses=True)
    
def lambda_handler(event, context):
    res = es.search(index='patients_v1', size=10000)
    
    for item in res['hits']['hits']:
        sensor_lst = item['_source']['sensors_list']
        if (sensor_lst != []):
            patient_id = item['_source']['patient_Id']
            for sensor in sensor_lst:
                redis_mapping.set(sensor['unit_Id'], patient_id)
                    