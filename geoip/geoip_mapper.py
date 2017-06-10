#!/opt/local/bin/python2.7
# coding=utf-8

from __future__ import absolute_import

import sys
import argparse
import logging

import apache_beam as beam
from apache_beam.transforms.core import ParDo

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

class GeoIpFn(beam.DoFn):
    gi = None    
    
    def process(self, element):
        if self.gi is None:    
            from resources.loader import load_geoip  
            self.gi = load_geoip()
            
        tablerow = element
        
        client_ip = tablerow.get("ip")
        try:
            if client_ip:
                country = self.gi.country_code_by_addr(client_ip)
                tablerow["geoIpCountry"] = country
        except:
            print "not found"
            
        yield tablerow  

def schemaConvert(schemaFields):
    return ",".join(["%s:%s" % (f.name, f.field_type) for f in schemaFields])

def run(projectId, src_dataset, src_tablename, dest_dataset, dest_tablename, gcs_location_prefix, jobname):
    
    from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions, WorkerOptions
    
    dataset = bigquery.Client(project=projectId).dataset(src_dataset)
    src_table = dataset.table(src_tablename)
    src_table.reload()
    dest_schema = src_table.schema
    dest_schema.append(SchemaField('geoIpCountry', 'STRING')) # add custom field name
    
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = projectId
    google_cloud_options.job_name = jobname
    google_cloud_options.staging_location = gcs_location_prefix + 'staging'
    google_cloud_options.temp_location = gcs_location_prefix + 'temp'
    
    worker_options = options.view_as(WorkerOptions)
    worker_options.num_workers = 32
    worker_options.machine_type = "n1-standard-1" # https://cloud.google.com/compute/docs/machine-types
    worker_options.autoscaling_algorithm = "NONE" # "THROUGHPUT_BASED"
    
    setup_options = options.view_as(SetupOptions) 
    setup_options.setup_file = "./setup.py"
    setup_options.save_main_session = False 
    
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    
    with beam.Pipeline(options=options) as p:
        rows = (p | 'ReadBQ' >> beam.io.Read(beam.io.BigQuerySource(table=src_tablename, dataset=src_dataset))
                | 'GeoIP' >> beam.ParDo(GeoIpFn())
                )
        
        rows | 'WriteBQ' >> beam.io.Write(
            beam.io.BigQuerySink(
                table = dest_tablename, 
                dataset= dest_dataset,
                schema=schemaConvert(dest_schema),
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
        

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run(*sys.argv[1:])