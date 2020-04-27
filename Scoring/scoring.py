# -*- coding: utf-8 -*-
"""
Created on Thu Apr 24 14:45:08 2020

@author: Sara Ben Shabbat

scoring.py - Scoring every patient condition according to his sensor/s data.
"""
#!/usr/bin/env python3

import redis
import json
from elasticsearch import Elasticsearch
import time
import pandas as pd
import numpy as np

host = 'https://search-covid198-es-2-x6zr2th7oiq7sjzp653cs3k3xm.eu-west-1.es.amazonaws.com/'
region = 'eu-west-1'
es = Elasticsearch(
    hosts=host,
)
es.ping()

# Redis connection.
elasticache_endpoint = "cv19redis-001.d9jy7a.0001.euw1.cache.amazonaws.com"
r = redis.StrictRedis(host=elasticache_endpoint, port=6379, db=0)

# Nano seconds since epoch. (- same format is used in the kinesis.)
ns_epoch = time.time_ns() // 1000000

# Create df's that represent the measure values and severity.
df_breath = pd.DataFrame({'min': {0: 0, 1: 9, 2: 12, 3: 21, 4: 25},
                          'max': {0: 8.99, 1: 11.99, 2: 20.99, 3: 24.99, 4: 35},
                          'severity': {0: 3, 1: 1, 2: 0, 3: 2, 4: 3}, })
df_pso2 = pd.DataFrame({'min': {0: 0, 1: 92, 2: 94, 3: 96},
                        'max': {0: 91.99, 1: 93.99, 2: 95.99, 3: 100},
                        'severity': {0: 3, 1: 2, 2: 1, 3: 0}, })
df_BPM = pd.DataFrame({'min': {0: 0, 1: 41, 2: 51, 3: 91, 4: 111, 5: 131},
                       'max': {0: 40.99, 1: 50.99, 2: 90.99, 3: 110.99, 4: 130.99, 5: 200},
                       'severity': {0: 3, 1: 1, 2: 0, 3: 1, 4: 2, 5: 3}, })
df_BloodPressure = pd.DataFrame({'min': {0: 0, 1: 91, 2: 101, 3: 111, 4: 220},
                                 'max': {0: 90.99, 1: 100.99, 2: 110.99, 3: 219.99, 4: 300},
                                 'severity': {0: 3, 1: 2, 2: 1, 3: 0, 4: 3}, })
df_fever = pd.DataFrame({'min': {0: 0, 1: 35.1, 2: 36.1, 3: 37.8, 4: 38.1, 5: 39.1},
                         'max': {0: 35.09, 1: 36.09, 2: 37.79, 3: 38.09, 4: 39.09, 5: 45},
                         'severity': {0: 3, 1: 1, 2: 0, 3: 1, 4: 2, 5: 3}, })

df_breath_high_fever = pd.DataFrame({'min': {0: 0, 1: 6.9, 2: 9.4, 3: 18, 4: 21.3},
                                     'max': {0: 6.89, 1: 9.35, 2: 17.99, 3: 21.29, 4: 35},
                                     'severity': {0: 3, 1: 1, 2: 0, 3: 2, 4: 3}, })
df_BPM_high_fever = pd.DataFrame({'min': {0: 0, 1: 35, 2: 43.6, 3: 76.6, 4: 94, 5: 110.6},
                                  'max': {0: 34.99, 1: 42.59, 2: 76.59, 3: 93.59, 4: 110.59, 5: 200},
                                  'severity': {0: 3, 1: 1, 2: 0, 3: 1, 4: 2, 5: 3}, })
df_BloodPressure_high_fever = pd.DataFrame({'min': {0: 0, 1: 76.6, 2: 85.6, 3: 93.6, 4: 186.2},
                                            'max': {0: 76.59, 1: 85.99, 2: 93.5, 3: 186.19, 4: 300},
                                            'severity': {0: 3, 1: 2, 2: 1, 3: 0, 4: 3}, })

df_names = [df_breath, df_pso2, df_BloodPressure, df_BPM, df_fever]
df_names_low = [df_breath_high_fever, df_pso2, df_BloodPressure_high_fever, df_BPM_high_fever, df_fever]
measure_names = ['breath_rate', 'saturation', 'blood_pressure_h', 'bpm', 'fever']
score_record_names = ['BreathRate', 'SpO2', 'BloodPressure', 'BPM', 'Fever']
expired_event = [False] * len(r.hvals('last_update'))




def score_alert(prev_score, score_record, cnt):
    current_score = score_record['Score']['Total']
    desc = ''
    severity = 0
    # Deterioration
    if(current_score > prev_score):
        if(current_score >= 7):
            desc = 'Critical deterioration'
            severity = 3
        elif(current_score >= 5 and current_score <= 6):
            desc = 'Medium deterioration'
            severity = 2
        elif(current_score >= 2 and current_score <= 4):
            desc = 'Slight deterioration'
            severity = 1
    # Improvement
    elif(current_score < prev_score and current_score <= 2):
            desc = 'Improvement'
            severity = 0

    if(desc != ''):
        # If this id has been used in this index for expired event (- for the current patient.), modifying the id in a pre defined way for having a relation between the id's.
        # No way to have a generated id same as new_id, as I increase the id over the maximal char length.
        new_id = score_record['Id'] if expired_event[cnt] == False else score_record['Id'] + '012345'
        es.index(index='patient_event', id=new_id, body={'PatientId': score_record['PatientID'], 'Timestamp': score_record['Timestamp'], 'Event': desc, 'Severity': severity})


def get_prev_score(patient_id: str):
    minus_hours = int(ns_epoch) - 43200000
    search_body = {
        'size': 10000,
        '_source': ['Timestamp', 'Score'],
        'query': {
            'bool': {
                'must': [
                    {'match': {'PatientID': patient_id}},
                    {'range': {'Timestamp': {"gte": minus_hours}}}
                ]
            }
        }
    }
    my_list = es.search(index="patient_status", body=search_body)['hits']['hits']

    # Check if the current patient - has no previous records.
    if (my_list == []):
        return None

    data_sorted = sorted(my_list, key=lambda item: int(item['_source']['Timestamp']))
    return data_sorted[len(data_sorted) - 1]['_source']['Score']['Total']


def scoring_measure(df_in_use, priority_names, i) -> int:
    score = 0
    # If the value isn't in the correct format - number, return score of zero.
    try:
        score = np.dot(
                (priority_names[i][measure_names[i]] >= df_in_use[i]['min'].values) &
                (priority_names[i][measure_names[i]] <= df_in_use[i]['max'].values),
                df_in_use[i]['severity'].values
                )
    finally:
        return score


def initial_vars():
    return 0, 0, {}


def get_desired_data(record):
    record = json.loads(record)
    general_measure = record
    primary_measure = record['primery_priority']
    secondary_measure = record['secondery_priority']
    return record, general_measure, primary_measure,secondary_measure


def es_no_cache():
    # Refresh the ES indexes I use.
    es.indices.clear_cache(index='patient_status')
    es.indices.refresh(index="patient_status")

    es.indices.clear_cache(index='patient_event')
    es.indices.refresh(index="patient_event")


def expired_alert(id: str, patient_id:str, all_expired_measures: str):
    all_expired_measures = all_expired_measures[:len(all_expired_measures) - 2]
    es.index(index='patient_event', id=id, body={'PatientId': patient_id, 'Timestamp': ns_epoch,
                                                 'Event': 'Over 12 hours without receiving new information about ' + all_expired_measures + '.',
                                                 'Severity': 1})


def update_expired_measure(patient_id: str, measure: str)-> str:
    # Removing the expired measure from 'LastKnown' index.
    current_patient = r.hget('LastKnown', patient_id)
    current_patient = json.loads(current_patient)
    
    if(measure == 'breath_rate' or measure =='wheezing' or measure == 'cough_presence_rate' or measure == 'crackles'):
        current_patient['primery_priority'].pop(measure)
    else:
        current_patient['secondery_priority'].pop(measure)
    r.hset('LastKnown', patient_id, json.dumps(current_patient))

    return measure + ', '


def get_expired_status(measure_datetime) -> bool:
    # A function gets the last datetime a measure was measures, and check if the measure has been expired.
    diff = (ns_epoch - int(measure_datetime)) / 3600000

    if(diff >= 12):
        return True
    return False


def check_expired():
    measure_check = measure_names + ['wheezing'] + ['cough_presence_rate'] + ['crackles']

    # Counter for patients having a record in 'patinet_event', for expired measure. (- This record and also scoring status have to use the same id. if so, in score_alert - modifying the id.)
    cnt = 0

    last_update_records = r.hvals('last_update')
    for record in last_update_records:
        # Get the records belong to the current patient_id from 'last_update' and 'LastKnown' redis indexes.
        record = json.loads(record)
        last_known = r.hget('LastKnown', record['patientId'])
        last_known = json.loads(last_known)

        all_expired_measures = ''
        # Iterate over all measures I need to check the receiving data about them.
        for measure in measure_check:
            # Checks whether the measure has ever been measured for the current patient.
            if(measure in record['updates']):
            # Check if the measure hasn't been remove from 'LastKnown' redis index (- every measure in its appropiate location in the dictionary.)
            # If no, Check if the measure has been expired. If yes, update about an expired measure.
                if (measure == 'breath_rate' or measure == 'wheezing' or measure == 'cough_presence_rate' or measure == 'crackles'):
                    if(measure in  last_known['primery_priority']):
                        if(get_expired_status(record['updates'][measure]) == True):
                            all_expired_measures = all_expired_measures + update_expired_measure(record['patientId'],  measure)
                elif (measure in last_known['secondery_priority']):
                    if(get_expired_status(record['updates'][measure]) == True):
                        all_expired_measures = all_expired_measures + update_expired_measure(record['patientId'], measure)

        # If any of the measure is expired, issue a message to ES.
        if(all_expired_measures != ''):
            expired_alert(record['Id'],  record['patientId'],  all_expired_measures)
            expired_event[cnt] = True

        cnt = cnt + 1


def main_func():
    # If the function doesn't success, it doensn't prevent to score the measures.
    try:
        check_expired()
    except:
        pass

    cnt = 0
    # Getting the data from 'LastKnown' index in redis.
    records = r.hvals("LastKnown")

    # Iterate over the patients and scoring each one.
    for record in records:
        # Get the desired data about every patient.
        record, general_measure, primary_measure, secondary_measure = get_desired_data(record)

        # Creating the record (- document, in ES terminology.) for pushing to elastic patient_data table.
        score_record = {}
        score_record['Id'] = general_measure['Id']
        score_record['PatientID'] = general_measure['patientId']
        score_record['Timestamp'] = general_measure['timeTag']

        # Scoring patient measures.
        total_score, score_per_measure, measure_dict = initial_vars()

        # Check if this measure exists.
        if('age' in general_measure):
            if(general_measure['age'] < 65):
                if(general_measure['age'] < 44):
                    score_per_measure = 0
                else:
                    score_per_measure = 1
            else:
                score_per_measure = 3
            measure_dict['AgeScore'] = score_per_measure
            total_score = total_score + score_per_measure

        # Every iteration, I have to update the var; as, the content of the measures changes ang they're not updated.
        priority_names = [primary_measure, secondary_measure, secondary_measure, secondary_measure, secondary_measure]

        # Check if the patient's fever is high, if yes - we have to use other df's for the scoring.
        if('fever' in secondary_measure):
            if(secondary_measure['fever'] > 37.5):
                df_in_use = df_names_low
            else:
                df_in_use = df_names
        else:
            df_in_use = df_names

        for i in range(len(measure_names)):
            # Check if the measure exists.
            if(measure_names[i] in priority_names[i]):
                score_per_measure=scoring_measure(df_in_use, priority_names, i)
                measure_dict[score_record_names[i]] = np.int(score_per_measure.item())
                total_score = total_score + score_per_measure

        try:
            score_per_measure = 0
            # If one of 'wheezing', 'cough_presence_rate' or 'crackles' has a value - the score for it - 2 or 3, ant it'll be under 'respiratory_finding' in the dict.
            if('crackles' in primary_measure):
                score_per_measure = measure_dict['RespiratoryFindings'] = 0 if primary_measure['crackles'] == False else 3

            if(score_per_measure == 0 and 'wheezing' in primary_measure):
                score_per_measure = measure_dict['RespiratoryFindings'] = 0 if primary_measure['wheezing'] == False else 2
            if(score_per_measure == 0 and 'cough_presence_rate' in primary_measure):
                score_per_measure = measure_dict['RespiratoryFindings'] = 0 if primary_measure['cough_presence_rate'] <= 0 else 2
            total_score = total_score + score_per_measure
        except:
            pass

        if(type(total_score) is not int):
            total_score = np.int(total_score.item())
        measure_dict['Total'] = total_score
        score_record['Score'] = measure_dict

        # Get previous score for the current patient, and if necessary - issue an appropriate message.
        prev_score = get_prev_score(score_record['PatientID'])
        if(prev_score != None):
              score_alert(prev_score, score_record, cnt)

        # If now, I don't success to write to ES the current patient scoring, it doesn't prevent from me continueing the loop; I hope in the next iteration - it'll work well.
        try:
            # Writing the document to ES.
            es.index(index='patient_status', id=score_record['Id'], body=score_record)
        except:
            pass

        cnt = cnt + 1



main_func()

