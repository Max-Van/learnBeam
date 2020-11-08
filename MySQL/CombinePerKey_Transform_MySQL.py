"""A example to read records on mysql. """
"""Resut is same as SELECT country,avg(creditLimit) FROM classicmodels.customers group by country """

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_mysql.connector import splitters
from beam_mysql.connector.io import ReadFromMySQL


class ReadRecordsOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument("--host", dest="host", default="localhost")
        parser.add_value_provider_argument("--port", dest="port", default=3306)
        parser.add_value_provider_argument("--database", dest="database", default="classicmodels")
        parser.add_value_provider_argument("--query", dest="query", default="SELECT * FROM classicmodels.customers;")
        parser.add_value_provider_argument("--user", dest="user", default="max")
        parser.add_value_provider_argument("--password", dest="password", default="CRMS24680")
        parser.add_value_provider_argument("--output", dest="output", default="out-CombinePerKey-records-result")


class OnlySelectTwoFields(beam.DoFn):

  def process(self, element):
      ##filter_record = {}
      ##filter_record= {element['id'],element['name'],element['role_main']}
      ##filter_record= {'role_main':element['role_main'],'attack_max':element['attack_max']}
      filter_record= (element['country'],float(element['creditLimit']))
      ##filter_record = {'Name': 'Zara', 'Age': 7, 'Class': 'First'}
      return [filter_record]

def run():
    options = ReadRecordsOptions()

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
        | "only select 2 fields" >> beam.ParDo(OnlySelectTwoFields())
        | 'Get mean value per key' >> beam.combiners.Mean.PerKey()
        | "WriteToText" >> beam.io.WriteToText(options.output, file_name_suffix=".txt", shard_name_template="")
    )

    p.run().wait_until_finish()

if __name__ == "__main__":
    run()