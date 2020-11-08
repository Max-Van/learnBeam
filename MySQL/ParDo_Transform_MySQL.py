"""A example to read records from mySQL and only select three fields of entire row"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_mysql.connector import splitters
from beam_mysql.connector.io import ReadFromMySQL


class OnlySelectThreeFields(beam.DoFn):

  def process(self, element):
      ##filter_record = {}
      ##filter_record= {element['id'],element['name'],element['role_main']}
      filter_record= {'customerNumber':element['customerNumber'],'customerName':element['customerName'],'country' :element['country']}
      ##filter_record = {'Name': 'Zara', 'Age': 7, 'Class': 'First'}
      return [filter_record]

def is_USACustomer(mysqlrecords):
      return mysqlrecords['country'] == 'USA'

class ReadRecordsOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument("--host", dest="host", default="localhost")
        parser.add_value_provider_argument("--port", dest="port", default=3306)
        parser.add_value_provider_argument("--database", dest="database", default="classicmodels")
        parser.add_value_provider_argument("--query", dest="query", default="SELECT * FROM classicmodels.customers;")
        parser.add_value_provider_argument("--user", dest="user", default="max")
        parser.add_value_provider_argument("--password", dest="password", default="CRMS24680")
        parser.add_value_provider_argument("--output", dest="output", default="out-filtered-records-only-three-fields-result")

def run():
    options = ReadRecordsOptions()
    ## Create Pipeline     
    p = beam.Pipeline(options=options)

    read_from_mysql = ReadFromMySQL(
        query=options.query,
        host=options.host,
        database=options.database,
        user=options.user,
        password=options.password,
        port=options.port,
        splitter=splitters.NoSplitter(),  # you can select how to split query from splitters
    )

    (
        p
        | "ReadFromMySQL" >> read_from_mysql
        | "Only select customer from USA" >> beam.Filter(is_USACustomer)
        | "only select 3 fields" >> beam.ParDo(OnlySelectThreeFields())
        | "WriteToText" >> beam.io.WriteToText(options.output, file_name_suffix=".txt", shard_name_template="")
    )

    p.run().wait_until_finish()

if __name__ == "__main__":
    run()