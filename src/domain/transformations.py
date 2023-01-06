import apache_beam as beam

from models.transaction import Transaction

class CompositeTransform(beam.PTransform):
    def __parse_transaction(self, line):
        parts = line.split(',')
        return Transaction(timestamp=parts[0],
                        origin=parts[1],
                        destination=parts[2],
                        transaction_amount=float(parts[3]))
        
    def __filter_transaction_amount(self, transaction):
        return transaction.transaction_amount > 20

    def __exclude_transactions_before_2010(self, transaction):
        return transaction.timestamp[:4] >= '2010'

    def __sum_by_date(self, transaction):
        return (transaction.timestamp.split(' ')[0], transaction.transaction_amount)

    def __combine_sums(self, transaction):
        return (transaction[0], sum(transaction[1]))

    def expand(self, transactions):
        return (
            transactions
                | 'Parse transactions' >> beam.Map(self.__parse_transaction)
                | 'Filter transaction amount' >> beam.Filter(self.__filter_transaction_amount)
                | 'Exclude transactions before 2010' >> beam.Filter(self.__exclude_transactions_before_2010)
                | 'Sum by date' >> beam.Map(self.__sum_by_date)
                | 'Group by date' >> beam.GroupByKey()
                | 'Combine sums' >> beam.Map(self.__combine_sums)
        )
