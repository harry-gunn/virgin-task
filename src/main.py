from application import application
from apache_beam.options.pipeline_options import PipelineOptions


def main():
    input_path = 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'
    output_path = 'output/results.jsonl.gz'
    pipeline_options = PipelineOptions()

    new_app = application.Application(input_path, output_path, pipeline_options)
    new_app.run()

if __name__ == '__main__':
    main()
