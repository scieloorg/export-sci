#! /usr/bin/python
# encoding: utf-8

import codecs
import argparse
import string
import unicodedata

from pymongo import Connection
from pymongo.errors import AutoReconnect


def remove_accents(data):
    return ''.join(
        x for x in unicodedata.normalize('NFKD', data) if x in string.printable
        ).lower()


class Normalization(object):

    def __init__(self,
                 conversion_table,
                 mongodb_host='127.0.0.1',
                 mongodb_port=27017):
        try:
            conn = Connection(mongodb_host, int(mongodb_port))
        except AutoReconnect:
            raise AutoReconnect(
                "Failed connecting to MongoDB at {0}:{1}".format(mongodb_host,
                                                                 mongodb_port))

        db = conn['scielo_network']
        self._coll = db['articles']
        self._convt = self._load_conversion_table(conversion_table)

    def _load_conversion_table(self, conversion_table):
        """
        This method load the itens to be replaced. The expected format for the
        conversion_table is a CSV file with 2 values in each line separeted by
        '|' pipes. The first value represents the string to be found and the
        second the expected new value that will replace the old value.
        Ex:
            Bra|Brazil
            BR|Brazil
            Brasil|Brazil
        """
        try:
            f = codecs.open(conversion_table, encoding='utf-8')
        except IOError:
            raise IOError('No such file ({0}), please check the parameter conversion_table when running the application'.format(conversion_table))

        convt = {}
        for key in f:
            if key:
                skey = key.split('|')
                if len(skey) != 2:
                    raise ValueError('Some values ({0}) in the conversion table are invalid. Each line must have 2 values separeted with |'.format(key))
                convt[remove_accents(skey[1].strip().lower())] = skey[1].strip()
                convt[remove_accents(skey[0].strip())] = skey[1].strip()

        if len(convt) == 0:
            raise ValueError('The conversion table file, must have at least one valid line.')

        return convt

    def _load_records(self, field, fltr={}):
        records = self._coll.find(fltr, {'code': 1})
        codes = []
        for record in records:
            codes.append(record['code'])

        for code in codes:
            yield self._coll.find_one({'code': code}, {'code': 1, field: 1})

    def bulk_data_fix(self,
                      fltr={},
                      field=None,
                      subfield='_',
                      write_into=None):
        """
        This method is responsible to replace the values of the field/subfield
        given according to the conversion_table:
        conversion_table sample: {'find': 'replace'}
        fields sample: 'article.v400', 'title.v100', 'citation.v100'
        subfield sample: 'a', 'b', 'c'
        write_into samploe: 'x', 'y'..

        If write_into is given, the nomalization will be stored in a subfield
        named by the write_into parameter, otherwise the normalization will
        replace the value in the original subfield.
        """

        if not write_into:
            write_into = subfield

        if not field:
            raise ValueError('The field must be given ex: article.v70')

        sfield = field.split('.')
        if len(sfield) != 2:
            raise ValueError('The field must be context.field where context must be "article", "title" or "citation" ex: article.v70')

        context = sfield[0]
        context_field = sfield[1]

        if not context in ['article', 'title', 'citation']:
            raise ValueError('The field must be context.field where context must be "article", "title" or "citation" ex: article.v70')

        if not context_field:
            raise ValueError('The field must be context.field where context must be "article", "title" or "citation" ex: article.v70')

        records = self._load_records(field, fltr=fltr)

        normalized = {context: {context_field: {}}}

        print 'This processing may take a while, go home, take a shower and check the output file tomorrow!!!'
        not_found = set()
        for record in records:
            fixed = []
            if context_field in record[context]:
                for item in record[context][context_field]:
                    if subfield in item:
                        key = remove_accents(item[subfield].lower().strip())
                        try:
                            item[write_into] = self._convt[key]
                            fixed.append(True)
                        except KeyError:
                            not_found.add(u'{0}|{1}'.format(record['code'],
                                                            item[subfield]))
                            fixed.append(False)
                    else:
                        fixed.append(None)
                normalized[context][context_field][write_into] = fixed
                self._coll.update({'code': record['code']},
                                 {'$set': {field: record[context][context_field], 'normalized': normalized}})

        fl = codecs.open('notfound_{0}.{1}.txt'.format(field, write_into),
                         'w',
                         encoding='utf-8')
        for term in not_found:
            fl.write(u'{0}\r\n'.format(term))


def main(*args, **xargs):

    fltr = xargs['fltr']

    if not isinstance(fltr, dict):
        sfltr = xargs['fltr'].split(':')
        fltr = {sfltr[0]: sfltr[1]}

    norm = Normalization(conversion_table=xargs['conv_table'],
                         mongodb_host=xargs['mongodb_host'],
                         mongodb_port=xargs['mongodb_port'])

    norm.bulk_data_fix(fltr,
                       field=xargs['field'],
                       subfield=xargs['subfield'],
                       write_into=xargs['write_to_another_subfield'])


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Create an access report")
    parser.add_argument('--mongodb_host',
                        default='localhost',
                        help='The MongoDB host containing the database with SciELO Articles')
    parser.add_argument('--mongodb_port',
                        default=27017,
                        help='The MongoDB port containing the database with SciELO Articles')
    parser.add_argument('--fltr',
                        default={},
                        help='Define a filter where the fix must be applied ex: {"code": "S0034-74342010000300011"}. There are filter available for article, journal and issue level.')
    parser.add_argument('--field',
                        default=None,
                        help='Give the field path that will be fixed ex: article.v70')
    parser.add_argument('--subfield',
                        default='_',
                        help='Give the subfield key that will be fixed if applicable ex: p')
    parser.add_argument('--write_to_another_subfield',
                        default=None,
                        help='Give a subfield key that will be filled with the normalization data. To be used if you want to create a new subfield with the normalized data, without changing the original data. If None, it will take the subfield given at --subfield.')
    parser.add_argument('--conversion_table',
                        default='conversion_table.csv',
                        help='Give file name containg the conversion table. Must be the exact path for the file in the file system')
    args = parser.parse_args()

    main(mongodb_host=args.mongodb_host,
         mongodb_port=args.mongodb_port,
         fltr=args.fltr,
         field=args.field,
         subfield=args.subfield,
         write_to_another_subfield=args.write_to_another_subfield,
         conv_table=args.conversion_table)
