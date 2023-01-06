
## Requirments

`pip install apache-beam`
`pip install 'apache-beam[gcp]'`


## Running tests

I've had to run the tests with the following command, due to the Windows dev env I currently am using:

`python -m unittest .\tests\domain\transformation_test.py`


## Some next steps

- More testing
    - Each sub transformation could be tested
    - I/O testing
    - E2E testing
- Error handling
- Pull I/O out into a repository