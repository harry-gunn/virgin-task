

import unittest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

import sys
import os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from domain.transformations import CompositeTransform

class TestCompositeTransform(unittest.TestCase):

  def test_composite_transform(self):
    input_data = [
        '2009-01-09 02:54:25 UTC,wallet,wallet,1021101.99',

        '2017-01-01 04:22:23 UTC,wallet,wallet,19.95',
        '2017-01-01 04:22:23 UTC,wallet,wallet,100',

        '2017-03-18 14:09:16 UTC,wallet,wallet,222.22',
        '2017-03-18 14:10:44 UTC,wallet,wallet,111.11',

        '2017-08-31 17:00:09 UTC,wallet,wallet,13700000023.08',
    ]

    with TestPipeline() as p:
      input = p | beam.Create(input_data)
      output = input | CompositeTransform()

      assert_that(
        output,
        equal_to(
            [('2017-01-01', 100.0), ('2017-03-18', 333.33), ('2017-08-31', 13700000023.08)],
        ))

if __name__ == '__main__':
    unittest.main()