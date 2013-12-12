import urllib2
import re
from datetime import datetime
import os
import zipfile
from ftplib import FTP, error_perm

import pymongo
from pymongo import Connection
from porteira.porteira import Schema
from lxml import etree


def ftp_connect(ftp_host='localhost',
                user='anonymous',
                passwd='anonymous'):

    ftp = FTP(ftp_host)
    ftp.login(user=user, passwd=passwd)

    return ftp


def send_to_ftp(file_name,
                ftp_host='localhost',
                user='anonymous',
                passwd='anonymous'):

    ftp = ftp_connect(ftp_host=ftp_host, user=user, passwd=passwd)
    f = open('tmp/{0}'.format(file_name), 'rd')
    ftp.storbinary('STOR inbound/{0}'.format(file_name), f)
    f.close()
    ftp.quit()


def send_take_off_files_to_ftp(ftp_host='localhost',
                                user='anonymous',
                                passwd='anonymous',
                                remove_origin=False):

    ftp = ftp_connect(ftp_host=ftp_host, user=user, passwd=passwd)
    
    for fl in os.listdir('controller'):
        if fl.split('.')[-1] == 'del':
            f = open('controller/{0}'.format(fl), 'rd')
            ftp.storbinary('STOR inbound/{0}'.format(fl), f)
            f.close()
        if remove_origin:
            os.remove('controller/{0}'.format(fl))
    
    ftp.quit()


def get_sync_file_from_ftp(ftp_host='localhost',
                           user='anonymous',
                           passwd='anonymous',
                           remove_origin=False):

    ftp = ftp_connect(ftp_host=ftp_host, user=user, passwd=passwd)
    ftp.cwd('reports')
    report_files = ftp.nlst('SCIELO_ProcessedRecordIds*')
    with open('controller/validated_ids.txt', 'wb') as f:
        def callback(data):
            f.write(data)
        for report_file in report_files:
            ftp.retrbinary('RETR %s' % report_file, callback)

    ftp.quit()
    f.close()

    if remove_origin:
        for report_file in report_files:
            ftp.delete('report_file')


def get_to_update_file_from_ftp(ftp_host='localhost',
                                user='anonymous',
                                passwd='anonymous',
                                remove_origin=False):

    ftp = ftp_connect(ftp_host=ftp_host, user=user, passwd=passwd)
    ftp.cwd('controller')
    with open('controller/toupdate.txt', 'wb') as f:
        def callback(data):
            f.write(data)
        try:
            ftp.retrbinary('RETR %s' % 'toupdate.txt', callback)
        except error_perm:
            return None

    if remove_origin:
        ftp.delete('toupdate.txt')

    ftp.quit()
    f.close()


def get_keep_into_file_from_ftp(ftp_host='localhost',
                        user='anonymous',
                        passwd='anonymous',
                        remove_origin=False):

    ftp = ftp_connect(ftp_host=ftp_host, user=user, passwd=passwd)
    ftp.cwd('controller')
    with open('controller/keepinto.txt', 'wb') as f:
        def callback(data):
            f.write(data)
        try:
            ftp.retrbinary('RETR %s' % 'keepinto.txt', callback)
        except error_perm:
            return None

    if remove_origin:
        ftp.delete('keepinto.txt')

    ftp.quit()
    f.close()


def get_take_off_files_from_ftp(ftp_host='localhost',
                        user='anonymous',
                        passwd='anonymous',
                        remove_origin=False):

    ftp = ftp_connect(ftp_host=ftp_host, user=user, passwd=passwd)
    ftp.cwd('controller')
    report_files = ftp.nlst('takeoff_*.del')
    with open('controller/takeoff.txt', 'wb') as f:
        def callback(data):
            f.write(data)
        for report_file in report_files:
            ftp.retrbinary('RETR %s' % report_file, callback)

    if remove_origin:
        for report_file in report_files:
            ftp.delete(report_file)

    ftp.quit()
    f.close()


def load_pids_list_to_be_removed(coll):

    now = datetime.now().isoformat()[0:10].replace('-','')

    recorded_at = 'controller/SCIELO_DEL_{0}.del'.format(now)

    toremove = []

    with open(recorded_at, 'wb') as f:
        for line in open('controller/takeoff.txt', 'r'):
            sline = line.strip()
            toremove.append(sline)
            if len(sline) == 9:
                for reg in coll.find({'code_title': sline}, {'code': 1}):
                    f.write('SCIELO|{0}|Y\r\n'.format(reg['code']))
            else:
                f.write('SCIELO|{0}|Y\r\n'.format(sline))

        f.close()

    return toremove

def sync_validated_xml(coll, remove_origin=False):

    with open('controller/validated_ids.txt', 'r') as f:
        for pid in f:
            coll.update({'code': pid.strip()}, {
                '$set': {
                    'validated_scielo': 'True',
                    'validated_wos': 'True',
                    'sent_wos': 'True',
                    }
                })

    if remove_origin:
        os.remove('controller/validated_ids.txt')

def packing_zip(files):
    now = datetime.now().isoformat()[0:10]

    if not os.path.exists('tmp/'):
        os.makedirs('tmp/', 0755)

    target = 'tmp/scielo_{0}.zip'.format(now)

    with zipfile.ZipFile(target, 'w') as zipf:
        for xml_file in files:
            zipf.write('tmp/xml/{0}'.format(xml_file), arcname=xml_file)

    return target


def load_journals_list(journals_file='journals.txt'):
    # ISSN REGEX
    prog = re.compile('^[0-9]{4}-[0-9]{3}[0-9X]$')

    issns = []
    with open(journals_file, 'r') as f:
        index = 0
        for line in f:
            index = index + 1
            if not '#' in line.strip() and len(line.strip()) > 0:
                issn = line.strip().upper()
                issn = prog.search(issn)
                if issn:
                    issns.append(issn.group())
                else:
                    print "Please check you journal.txt file, the input '{0}' at line '{1}' is not a valid issn".format(line.strip(), index)

    if len(issns) > 0:
        return issns
    else:
        return None


def get_collection(mongodb_host='localhost',
               mongodb_port=27017,
               mongodb_database='scielo_network',
               mongodb_collection='articles'):

    conn = Connection(mongodb_host, mongodb_port)
    db = conn[mongodb_database]
    coll = db[mongodb_collection]
    coll.ensure_index([('code_title', pymongo.ASCENDING),
                       ('validated_scielo', pymongo.ASCENDING),
                       ('applicable', pymongo.ASCENDING),
                       ('sent_wos', pymongo.ASCENDING),
                       ('publication_year', pymongo.ASCENDING)])
    coll.ensure_index([('code_title', pymongo.ASCENDING),
                       ('validated_scielo', pymongo.ASCENDING),
                       ('sent_wos', pymongo.ASCENDING),
                       ('publication_year', pymongo.ASCENDING)])
    coll.ensure_index([('code_title', pymongo.ASCENDING),
                       ('sent_wos', pymongo.ASCENDING)])
    coll.ensure_index([('code_title', pymongo.ASCENDING),
                       ('validated_scielo', pymongo.ASCENDING)])
    coll.ensure_index([('code_title', pymongo.ASCENDING),
                       ('validated_wos', pymongo.ASCENDING)])
    coll.ensure_index([('validated_wos', pymongo.ASCENDING)])
    coll.ensure_index([('validated_scielo', pymongo.ASCENDING)])
    coll.ensure_index([('sent_wos', pymongo.ASCENDING)])
    coll.ensure_index('code')
    coll.ensure_index('code_title')
    coll.ensure_index('code_issue')
    coll.ensure_index('applicable')
    coll.ensure_index('article_title_md5')
    coll.ensure_index('article_title_no_accents')
    coll.ensure_index('citations_title_no_accents')
    coll.ensure_index('article_title_author_year_no_accents')
    coll.ensure_index('citations_title_author_year_no_accents')
    coll.ensure_index('article.doi')

    return coll


def write_log(article_id, issue_id, schema, xml, msg):
    now = datetime.now().isoformat()[0:10]
    error_report = open("reports/{0}_{1}_errors.txt".format(issue_id, now), "a")
    error_msg = "{0}: {1}\r\n".format(article_id, str(schema.get_validation_errors(xml)))
    error_report.write(error_msg)
    error_report.close()


def validate_xml(coll, article_id, issue_id, api_host='localhost', api_port='7000'):
    """
    Validate article agains WOS Schema. Flaging his attribute validated_scielo to True if
    the document is valid.
    """
    xsd = open('ThomsonReuters_publishing.xsd', 'r').read()
    sch = Schema(xsd)

    xml_url = 'http://{0}:{1}/api/v1/article?code={2}&format=xml&show_citation=True'.format(api_host, api_port, article_id)

    xml = urllib2.urlopen(xml_url, timeout=30).read()

    try:
        result = sch.validate(xml)
    except etree.XMLSyntaxError as e:
        msg = "{0}: Problems reading de XML, {1}".format(article_id, e.text)
        write_log(article_id,
                  issue_id,
                  sch,
                  xml,
                  msg)

        return None

    if result:
        coll.update({'code': article_id}, {'$set': {'validated_scielo': 'True'}})
        return xml
    else:
        msg = ""

        for error in sch.get_validation_errors(xml):
            msg += "{0}: {1}\r\n".format(article_id, error[2])

        write_log(article_id,
                  issue_id,
                  sch,
                  xml,
                  msg)

    return None


def find(fltr, collection, skip, limit):

    return collection.find(fltr, {'code': 1}).skip(skip).limit(limit)

    #for article in collection.find(fltr, {'code': 1}).skip(skip).limit(limit):
        #yield article['code']


def not_send(collection,
                  code_title=None,
                  publication_year=1800,
                  skip=0,
                  limit=10000):
    """
    Implements an iterable article PID list not validated on SciELO.
    validated_scielo = False
    sent_to_wos = False
    """

    fltr = {'sent_wos': 'False',
            'applicable': 'True',
            'publication_year': {'$gte': str(publication_year)}}

    if code_title:
        fltr.update({'code_title': code_title})

    return find(fltr, collection, skip=skip, limit=limit)


def validated(collection,
              code_title=None,
              publication_year=1800,
              skip=0,
              limit=10000):
    """
    Implements an iterable article PID list eligible to be send to WoS.
    validated_scielo = True
    sent_to_wos = False
    """

    fltr = {'sent_wos': 'True',
            'validated_scielo': 'False',
            'publication_year': {'$gte': str(publication_year)}}

    if code_title:
        fltr.update({'code_title': code_title})

    return find(fltr, collection, skip=skip, limit=limit)


def sent_to_wos(collection,
                code_title=None,
                publication_year=1800,
                skip=0,
                limit=10000):
    """
    Implements an iterable article PID list cotaining docs already sento to wos.
    sent_wos = True
    """

    fltr = {'sent_wos': 'True',
            'publication_year': {'$gte': str(publication_year)}}

    if code_title:
        fltr.update({'code_title': code_title})

    return find(fltr, collection, skip=skip, limit=limit)


def validated_wos(collection,
                code_title=None,
                publication_year=1800,
                skip=0,
                limit=10000):
    """
    Implements an iterable article PID list cotaining docs already sento to wos.
    sent_wos = True
    """

    fltr = {'validated_wos': 'True',
            'publication_year': {'$gte': str(publication_year)}}

    if code_title:
        fltr.update({'code_title': code_title})

    return find(fltr, collection, skip=skip, limit=limit)
