import apache_beam as beam
from domain.transformations import CompositeTransform

class Application:
    def __init__(self, input_path, output_path, pipeline_options):
        self.input_path = input_path
        self.output_path = output_path
        self.pipeline_options = pipeline_options

    def run(self):
        with beam.Pipeline(options=self.pipeline_options) as p:
            transactions = p | beam.io.ReadFromText(self.input_path, skip_header_lines=1)
            transformed_data = transactions | CompositeTransform()
            transformed_data | 'Write results' >> beam.io.WriteToText(self.output_path)

