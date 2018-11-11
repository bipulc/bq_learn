#! /usr/bin/env python

# Import required packages
import requests
import json
import datetime
import time
import re
from google.cloud import bigquery

def loadDatatoBQ(fname, datasetid, tableid):
    '''
    Function to load data into BigQuery table reading from a local file.
    Job Config will be built here.. and same for all table load
    '''
    
    # Count number of lines in the file to set max_bad_records parameter below
    num_lines = sum(1 for line in open(fname))

    client = bigquery.Client()
    dataset_ref = client.dataset(datasetid)
    table_ref = dataset_ref.table(tableid)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = False
    job_config.create_disposition = bigquery.job.CreateDisposition.CREATE_NEVER
    job_config.max_bad_records = num_lines
    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND

    with open(fname ,'rb') as source_file:
        job = client.load_table_from_file(source_file, table_ref, location='US', job_config=job_config)
    try:
        job.result()
        print('Loaded {} rows into {}:{}'.format(job.output_rows, datasetid, tableid))
        print('Job id {}'.format(job.job_id))
        # if the file loading encounters error, then open the source file for printing bad records
        if len(job.errors) > 0:
            print('number of errors {}'.format(len(job.errors)))
            fp = open(fname,'r')
        
        for i in range(0,len(job.errors)):
            print('Error # - {}  Error Message - {}'.format(i,job.errors[i]['message']))
            # Get the location of error
            err_loc = re.findall(r'\d+', job.errors[i]['message'])
            print int(err_loc[0])
            # see to error location
            try:
                fp.seek(int(err_loc[0]))
                print('Input data - {}'.format(fp.readline()))
            except:
                raise
    except:
        print('Job id {}'.format(job.job_id))
        print('number of errors {}'.format(len(job.errors)))
        raise

if __name__ == "__main__":

    cfname = 'person_tab_data.json'

    datasetid = 'bq_learn'
    
    tableid = 't_person_info'
    print('Loading from file {} into table {}'.format(cfname, tableid))
    loadDatatoBQ(cfname,datasetid,tableid)
