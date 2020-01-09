# coding: utf-8
import re
from datetime import datetime
import os
import shutil
import zipfile
from ftplib import FTP, error_perm, all_errors
import logging
import contextlib

import requests
from pymongo import MongoClient
from lxml import etree
from StringIO import StringIO


# SciELO article types stored in field v71 that are allowed to be sent to WoS
wos_article_types = ['ab', 'an', 'ax', 'co', 'cr', 'ct', 'ed', 'er', 'in',
                     'le', 'mt', 'nd', 'oa', 'pr', 'pv', 'rc', 'rn', 'ra',
                     'sc', 'tr', 'up']

wos_collections_allowed = ['scl', 'arg', 'cub', 'esp', 'col', 'ven', 'chl', 
    'sza', 'prt', 'pry', 'cri', 'per', 'mex', 'ury', 'bol']

XML_ERRORS_ROOT_PATH = 'xml_errors'


def delete_file_or_folder(path):
    if os.path.isdir(path):
        for item in os.listdir(path):
            delete_file_or_folder(path + '/' + item)
        try:
            shutil.rmtree(path)
        except:
            logging.info('Unable to delete: %s' % path)

    elif os.path.isfile(path):
        try:
            os.unlink(path)
        except:
            logging.info('Unable to delete: %s' % path)


class FTPService(object):

    def __init__(
            self,
            host='localhost',
            port='21',
            user='anonymous',
            passwd='anonymous'):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.ftp = None

    def connect(self, timeout=60):
        if self.ftp is None:
            self.ftp = FTP()
        self.ftp.connect(self.host, self.port, timeout=timeout)
        self.ftp.login(user=self.user, passwd=self.passwd)

    def close(self):
        try:
            self.ftp.quit()
        except:
            self.ftp.close()

    @contextlib.contextmanager
    def session_context(self, timeout=60):
        self.connect(timeout)
        yield
        self.close()

    def mkdirs(self, dirs, timeout=60):
        with self.session_context(timeout):
            folders = dirs.split('/')
            for folder in folders:
                try:
                    self.ftp.mkd(folder)
                except:
                    logging.info('FTP: MKD (%s)' % (dirs, ), exc_info=True)
                self.ftp.cwd(folder)

    def send_file(self, local_filename, remote_filename, timeout=60):
        with self.session_context(timeout):
            f = open(local_filename, 'rb')
            try:
                self.ftp.storbinary('STOR {}'.format(remote_filename), f)
            except all_errors:
                logging.info(
                    'FTP: Unable to send %s to %s' %
                    (local_filename, remote_filename), exc_info=True)
            f.close()


class CollectionReports(object):

    def __init__(self, collection_name, reports_root_path, zips_root_path):
        _date = datetime.now().isoformat()[:16]
        _date = _date[:10]
        _date = _date.replace(':', '').replace('-', '').replace('T', '_')
        self.collection_name = collection_name
        self.collection_reports_path = os.path.join(
            reports_root_path, collection_name)
        self.zipname_local = collection_name+'.zip'
        self.zipname_remote = collection_name+'_'+_date+'.zip'
        self.zip_filename = os.path.join(
            zips_root_path, self.zipname_local)

    def list(self):
        rep_files = []
        if os.path.isdir(self.collection_reports_path):
            for issn in os.listdir(self.collection_reports_path):
                d = os.path.join(self.collection_reports_path, issn)
                if os.path.isdir(d):
                    for f in os.listdir(d):
                        filename = os.path.join(d, f)
                        if os.path.isfile(filename):
                            rep_files.append('{}/{}/{}'.format(
                                    self.collection_name,
                                    issn,
                                    f))
        return rep_files

    def zip(self, delete=False):
        root_path = os.path.dirname(self.collection_reports_path)
        rep_files = self.list()
        delete_file_or_folder(self.zip_filename)
        update_zipfile(self.zip_filename, rep_files, root_path, delete=delete)
        if delete:
            delete_file_or_folder(self.collection_reports_path)

    def ftp(self, ftp_service, remote_root_path, delete=False):
        logging.info('ftp.send %s' % self.zip_filename)
        if os.path.isfile(self.zip_filename):

            logging.info('ftp.mkdirs %s' % remote_root_path)
            ftp_service.mkdirs(remote_root_path)

            remote = os.path.join(remote_root_path, self.zipname_remote)
            logging.info(
                'ftp.send_file %s to %s' % (self.zip_filename, remote))
            sent = ftp_service.send_file(self.zip_filename, remote)
            if sent is not False and delete:
                delete_file_or_folder(self.zip_filename)


def send_collections_reports(ftp_host, ftp_user, ftp_passwd,
                             local_path='collections_reports',
                             remote_path='collections_reports'):
    ftp_service = FTPService(ftp_host, user=ftp_user, passwd=ftp_passwd)
    reports_root_path = XML_ERRORS_ROOT_PATH

    zips_root_path = local_path
    if not os.path.isdir(zips_root_path):
        os.makedirs(zips_root_path)

    for collection_name in os.listdir(reports_root_path):
        print(collection_name)
        path = os.path.join(reports_root_path, collection_name)
        if os.path.isdir(path):
            reports = CollectionReports(
                        collection_name, reports_root_path, zips_root_path)
            reports.zip(delete=False)
            reports.ftp(ftp_service, remote_path, delete=True)


def update_zipfile(zip_filename, files, src_path, mode='a', delete=False):
    with zipfile.ZipFile(
            zip_filename,
            mode,
            compression=zipfile.ZIP_DEFLATED,
            allowZip64=True) as zipf:
        for f in files:
            src = os.path.join(src_path, f)
            zipf.write(src, arcname=f)
            if delete is True:
                delete_file_or_folder(src)
    logging.info('Files zipped into: %s' % zip_filename)


def write_file(filename, content, mode='w'):
    content = content.encode('utf-8')
    with open(filename, mode) as f:
        try:
            f.write(content)
        except (IOError, ValueError):
            logging.error('Error writing file: %s' % filename, exc_info=True)
        except Exception as e:
            logging.exception('tools.write_file(): %s' % filename, e)


def write_log(msg):
    now = datetime.now().isoformat()[0:10]
    issn = msg.split(':')[1][1:10]
    if not os.path.isdir("reports"):
        os.makedirs("reports")
    filename = "reports/{0}_{1}_errors.txt".format(issn, now)
    error_report = open(filename, "a")
    msg = u'%s\r\n' % msg
    try:
        error_report.write(msg.encode('utf-8'))
    except Exception as e:
        logging.exception('tools.write_log(%s): ' % filename, e)

    error_report.close()


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

    now = datetime.now().isoformat()[0:10]

    target = 'scielo_{0}.zip'.format(now)

    ftp = ftp_connect(ftp_host=ftp_host, user=user, passwd=passwd)
    f = open('{0}'.format(file_name), 'rb')
    ftp.storbinary('STOR inbound/{0}'.format(target), f)
    f.close()
    ftp.quit()
    logging.debug('file sent to ftp: %s' % target)

    send_collections_reports(ftp_host, user, passwd)


def send_take_off_files_to_ftp(ftp_host='localhost',
                               user='anonymous',
                               passwd='anonymous',
                               remove_origin=False):

    ftp = ftp_connect(ftp_host=ftp_host, user=user, passwd=passwd)

    for fl in os.listdir('controller'):
        if fl.split('.')[-1] == 'del':
            f = open('controller/{0}'.format(fl), 'rb')
            ftp.storbinary('STOR inbound/{0}'.format(fl), f)
            f.close()
            logging.debug('Takeoff file sent to ftp: %s' % fl)

            if remove_origin:
                os.remove('controller/{0}'.format(fl))
                logging.debug('Takeoff file removed from origin: %s' % fl)

    ftp.quit()


def remove_previous_unbound_files_from_ftp(ftp_host='localhost',
                           user='anonymous',
                           passwd='anonymous',
                           remove_origin=False):

    ftp = ftp_connect(ftp_host=ftp_host, user=user, passwd=passwd)
    ftp.cwd('inbound')
    report_files = ftp.nlst('*')

    for report_file in report_files:
        logging.debug('Previous unbound files removed from ftp: %s' % report_file)
        ftp.delete(report_file)


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
            logging.debug('Syncronization files removed from ftp: %s' % report_file)
            ftp.delete(report_file)


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


def packing_zip(files):
    now = datetime.now().isoformat()[0:10]

    target = 'scielo_{0}.zip'.format(now)

    logging.info('zipping XML files to: %s' % target)

    with zipfile.ZipFile(target, 'w', compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zipf:
        for xml_file in files:
            zipf.write('xml/{0}'.format(xml_file), arcname=xml_file)

    logging.debug('Files zipped into: %s' % target)

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
                    logging.debug("Please check you journal.txt file, the input '{0}' at line '{1}' is not a valid issn".format(line.strip(), index))

    if len(issns) > 0:
        return issns
    else:
        return None


class XMLValidator(object):

    def __init__(self):
        xsd_filename = os.path.abspath(
                            os.path.join(
                                os.path.dirname(__file__),
                                'xsd/ThomsonReuters_publishing.xsd'))
        self.validator = XMLValidatorWithSchema(xsd_filename)
        self.articlemeta_url = 'http://articlemeta.scielo.org/api/v1/article'

    def _get_xml(self, collection, code):
        params = {'collection': collection,
                  'code': code,
                  'format': 'xmlwos'}
        try:
            return requests.get(
                self.articlemeta_url, params=params, timeout=30).text
        except (
                requests.ConnectionError,
                requests.HTTPError,
                requests.Timeout):
            logging.info(
                'error fetching url: %s' % self.articlemeta_url)

    def validated_xml(self, textxml):
        validated = ValidatedXML(textxml)
        validated.validate(self.validator)
        return validated

    def validate_xml(self, collection, code):
        textxml = self._get_xml(collection, code)
        validated_xml = self.validated_xml(textxml)
        article_report = ArticleReport(
                            self.articlemeta_url,
                            collection,
                            code,
                            XML_ERRORS_ROOT_PATH)
        article_report.save(validated_xml)
        if validated_xml.errors is None or len(validated_xml.errors) == 0:
            return validated_xml.tree


class XML(object):

    def __init__(self, textxml):
        self.parse_errors = []
        self.text = textxml
        self._parse_xml()

    def _parse_xml(self):
        xml = StringIO(self.text)
        self.tree = None
        try:
            self.tree = etree.parse(xml)
        except etree.XMLSyntaxError as e:
            self.parse_errors.append(e.message)
        except Exception as e:
            msg = 'tools.XML._parse_xml(): Unknown error. '
            logging.exception(msg, e)
            self.parse_errors.append(msg)

    @property
    def pretty_text(self):
        if self.tree is None:
            return self.text.replace('<', '\n<').replace('\n</', '</').strip()
        return etree.tostring(
                self.tree,
                encoding='unicode',
                pretty_print=True)


class XMLValidatorWithSchema(object):

    def __init__(self, xsd_filename):
        self.xml_schema = xsd_filename

    @property
    def xml_schema(self):
        return self._xml_schema

    @xml_schema.setter
    def xml_schema(self, xsd_filename):
        try:
            with open(xsd_filename, 'r') as str_schema:
                schema_doc = etree.parse(str_schema)
                self._xml_schema = etree.XMLSchema(schema_doc)
        except (IOError, ValueError, etree.XMLSchemaError) as e:
            logging.exception('tools.XMLValidatorWithSchema.xml_schema', e)

    def validate(self, tree):
        if self.xml_schema is None:
            return 'XMLSchema is not loaded'

        try:
            self.xml_schema.validate(tree)
        except etree.XMLSyntaxError as e:
            return e.message
        except Exception as e:
            logging.exception('tools.XMLValidatorWithSchema.validate', e)

        try:
            self.xml_schema.assertValid(tree)
        except etree.DocumentInvalid as e:
            return e.message
        except Exception as e:
            logging.exception('tools.XMLValidatorWithSchema.assertValid', e)


class ValidatedXML(object):

    def __init__(self, textxml):
        self._errors = []
        self._original_xml = None
        self._pretty_xml = None
        if textxml is None:
            self.errors = ['Empty XML']
        else:
            self._original_xml = XML(textxml)
            self._pretty_xml = XML(self._original_xml.pretty_text)
            self.errors = self._pretty_xml.parse_errors

    @property
    def tree(self):
        if self._original_xml is not None:
            return self._original_xml.tree

    @property
    def errors(self):
        return self._errors

    @errors.setter
    def errors(self, messages):
        if messages is not None:
            if isinstance(messages, list):
                self._errors.extend(messages)
            else:
                self._errors.append(messages)

    def validate(self, validate_with_schema=None):
        if len(self.errors) == 0:
            if validate_with_schema is not None:
                self.errors = validate_with_schema.validate(
                    self._pretty_xml.tree)

    def display(self, numbered_lines=False):
        if self._original_xml is not None:
            if numbered_lines:
                lines = self._original_xml.pretty_text.split('\n')
                nlines = len(lines)
                digits = len(str(nlines))
                return '\n'.join(
                    [u'{}:{}'.format(str(n).zfill(digits), line)
                     for n, line in zip(range(1, nlines), lines)])
            return self._original_xml.pretty_text


class ArticleReport(object):

    def __init__(self, article_uri, collection, code, xml_error_root_path):
        self.collection = collection
        self.code = code
        self.xml_error_root_path = xml_error_root_path
        self.report_filename = '{}/{}.err.txt'.format(
            self.issn_path, self.code)
        self.url = '{}/?collection={}&code={}&format=xmlwos\n'.format(
                    article_uri, self.collection, self.code)

    @property
    def issn_path(self):
        issn = self.code[1:10]
        path = '{}/{}/{}'.format(
            self.xml_error_root_path, self.collection, issn)
        if not os.path.isdir(path):
            os.makedirs(path)
        return path

    def save(self, validated, numbered=False):
        now = datetime.now().isoformat()
        if validated.errors is None or len(validated.errors) == 0:
            return delete_file_or_folder(self.report_filename)
        errors = '\n'.join(validated.errors)
        sep = '\n'*2
        content = []
        xml = validated.display(numbered)
        if numbered:
            content = [now, self.url, 'ERRORS\n'+'='*6, errors, '-'*30, xml]
        else:
            content = [xml, '-'*30, now, self.url, 'ERRORS\n'+'='*6, errors]

        write_file(self.report_filename, sep.join(content))


class DataHandler(object):

    def __init__(
        self,
        mongodb_host='localhost',
        mongodb_port=27017,
        mongodb_database='articlemeta',
        mongodb_collection='articles'
    ):

        db = MongoClient(mongodb_host)[mongodb_database]

        self._articles_coll = self._set_articles_coll(db)
        self._collections_coll = self._set_collections_coll(db)

    def _set_articles_coll(self, db):

        coll = db['articles']
        coll.ensure_index('publication_year')
        coll.ensure_index('sent_wos')
        coll.ensure_index('applicable')

        return coll

    def _set_collections_coll(self, db):

        coll = db['collections']
        coll.ensure_index('code')

        return coll

    def load_pids_list_to_be_removed(self):

        now = datetime.now().isoformat()[0:10].replace('-', '')

        recorded_at = 'controller/SCIELO_DEL_{0}.del'.format(now)

        toremove = []

        with open(recorded_at, 'wb') as f:
            for line in open('controller/takeoff.txt', 'r'):
                sline = line.strip()
                toremove.append(sline)
                if len(sline) == 9:
                    for reg in self._articles_coll.find({'code_title': sline}, {'code': 1}):
                        f.write('SCIELO,{0},Y\r\n'.format(reg['code']))
                else:
                    f.write('SCIELO,{0},Y\r\n'.format(sline))

            f.close()

        return toremove


    def sync_sent_documents(self, remove_origin=False):

        with open('controller/validated_ids.txt', 'r') as f:
            for pid in f:
                self._articles_coll.update(
                    {'code': pid.strip()}, {'$set': {'sent_wos': 'True'}}, multi=True
                )

        if remove_origin:
            os.remove('controller/validated_ids.txt')


    def load_collections_metadata(self):

        collections = self._collections_coll.find()

        dict_collections = {}
        for collection in collections:
            dict_collections.setdefault(collection['code'], collection)

        return dict_collections

    def set_elegible_document_types(self):
        documents = self._articles_coll.find({'applicable': 'False'}, {'collection': 1, 'code': 1, 'article.v71': 1})

        for document in documents:

            if not 'v71' in document['article']:
                continue

            if document['article']['v71'][0]['_'] in wos_article_types:
                self._articles_coll.update(
                    {'collection': document['collection'], 'code': document['code']}, {'$set': {'applicable': 'True'}}
                )

    def not_sent(self, code_title=None, publication_year=1800):
        """
        Implements an iterable article PID list not validated on SciELO.
        sent_wos = False
        """

        fltr = {'sent_wos': 'False',
                'applicable': 'True',
                'collection': {'$in': wos_collections_allowed},
                'publication_year': {'$gte': str(publication_year)}}

        if code_title:
            fltr.update({'code_title': code_title})

        documents = []
        total = 0
        for document in self._articles_coll.find(fltr, {'collection':1, 'code': 1}):
            total += 1
            documents.append([document['collection'], document['code']])

        i = 0
        for document in documents:
            i = i + 1
            yield [total, i, self._articles_coll.find_one({'collection': document[0], 'code': document[1]}, {'citations': 0})]

    def sent_to_wos(self, code_title=None):
        """
        Implements an iterable article PID list cotaining docs already sent to wos.
        sent_wos = True
        """

        fltr = {'sent_wos': 'True'}

        if code_title:
            fltr.update({'code_title': code_title})

        documents = []
        total = 0
        for document in self._articles_coll.find(fltr, {'code': 1}):
            total += 1
            documents.append([document['collection'], document['code']])

        i = 0
        for document in documents:
            i += 1
            yield [total, i, self_articles_coll.find_one({'collection': document[0], 'code': document[1]}, {'citations': 0})]
