# coding: utf-8

from datetime import datetime
import os
import argparse
import logging

import tools
import utils
from lxml import etree

logger = logging.getLogger(__name__)
config = utils.Configuration.from_env()
settings = dict(config.items())['main:exportsci']

FTP_HOST = settings['ftp_host']
FTP_USER = settings['ftp_user']
FTP_PASSWD = settings['ftp_passwd']
MONGODB_HOST = settings['mongodb_host']
MONGODB_SLAVEOK = bool(settings['mongodb_slaveok'])


def _config_logging(logging_level='INFO', logging_file=None):

    allowed_levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    logger.setLevel(allowed_levels.get(logging_level, 'INFO'))

    if logging_file:
        hl = logging.FileHandler(logging_file, mode='a')
    else:
        hl = logging.StreamHandler()

    hl.setFormatter(formatter)
    hl.setLevel(allowed_levels.get(logging_level, 'INFO'))

    logger.addHandler(hl)

    return logger


def run(task='add', clean_garbage=False, normalize=True):
    working_dir = os.listdir('.')
    logger.debug('Validating working directory %s' % working_dir) 
    if not 'controller' in working_dir:
        logger.error('Working dir does not have controller directory')
        exit()

    if not 'reports' in working_dir:
        logger.error('Working dir does not have reports directory')
        exit()

    if not 'xml' in working_dir:
        logger.error('Working dir does not have xml directory')
        exit()

    # Setup a connection to SciELO Network Collection
    logger.debug("Connecting to mongodb with DataHandler thru %s" % (MONGODB_HOST))

    dh = tools.DataHandler(MONGODB_HOST)

    now = datetime.now().isoformat()[0:10]
    index_issn = 0

    collections = dh.load_collections_metadata()

    if clean_garbage:
        logger.debug("Removing previous XML files")
        os.system('rm -f xml/*.xml')
        logger.debug("Removing previous zip files")
        os.system('rm -f *.zip')
        logger.debug("Removing previous error report files")
        os.system('rm -f report/*errors.txt')

    if task == 'update':
        logger.debug("Loading toupdate.txt ISSN's file from FTP controller directory")
        tools.get_to_update_file_from_ftp(ftp_host=FTP_HOST,
                                          user=FTP_USER,
                                          passwd=FTP_PASSWD)

        issns = tools.load_journals_list(journals_file='controller/toupdate.txt')

    elif task == 'add':
        logger.debug("Loading keepinto.txt ISSN's file from FTP controller directory")
        tools.get_keep_into_file_from_ftp(ftp_host=FTP_HOST,
                                          user=FTP_USER,
                                          passwd=FTP_PASSWD)

        issns = tools.load_journals_list(journals_file='controller/keepinto.txt')

    logger.debug("Remove previous inbound files")
    tools.remove_previous_unbound_files_from_ftp(ftp_host=FTP_HOST,
                                 user=FTP_USER,
                                 passwd=FTP_PASSWD)

    logger.debug("Syncing XML's status according to WoS validated files")
    tools.get_sync_file_from_ftp(ftp_host=FTP_HOST,
                                 user=FTP_USER,
                                 passwd=FTP_PASSWD)

    dh.sync_sent_documents(remove_origin=clean_garbage)

    logger.debug("Creating file with a list of documents to be removed from WoS")
    tools.get_take_off_files_from_ftp(ftp_host=FTP_HOST,
                                      user=FTP_USER,
                                      passwd=FTP_PASSWD,
                                      remove_origin=clean_garbage)

    ids_to_remove = dh.load_pids_list_to_be_removed()

    tools.send_take_off_files_to_ftp(ftp_host=FTP_HOST,
                                     user=FTP_USER,
                                     passwd=FTP_PASSWD,
                                     remove_origin=clean_garbage)

    logger.debug("Defining document types elegible to send to SCI")
    dh.set_elegible_document_types()

    xml_validator = tools.XMLValidator()

    # Loading XML files
    for issn in issns:
        index_issn = index_issn + 1

        if issn in ids_to_remove:
            logger.debug("Issn {0} is available in the takeoff and keepinto file. For now this ISSN was ignored, and will not be send to WoS until it is removed from the takeoff file.".format(issn))
            continue

        if task == 'update':
            documents = dh.sent_to_wos(issn)
            xml_file_name = "xml/SciELO_COR_{0}_{1}.xml".format(now, issn)
        elif task == 'add':
            documents = dh.not_sent(issn, publication_year=2002)
            xml_file_name = "xml/SciELO_{0}_{1}.xml".format(now, issn)

        if not os.path.exists('xml'):
            os.makedirs('xml')

        nsmap = {
            'xml': 'http://www.w3.org/XML/1998/namespace',
            'xlink': 'http://www.w3.org/1999/xlink'
        }
        global_xml = etree.Element('articles', nsmap=nsmap)
        global_xml.set('dtd-version', '1.10')
        global_xml.set('{http://www.w3.org/2001/XMLSchema-instance}noNamespaceSchemaLocation', 'ThomsonReuters_publishing_1.10.xsd')

        if os.path.exists(xml_file_name):
            logger.warning("File {0} already exists".format(xml_file_name))
            continue

        for total, current, document in documents:
            try:
                if current == 1:
                    logger.info("validating xml's {0} for {1}".format(total, issn))

                logger.info("validating xml {0}/{1}".format(current, total))

                #skip ahead documents
                if 'v32' in document['article'] and 'ahead' in document['article']['v32'][0]['_'].lower():
                    continue

                xml = xml_validator.validate_xml(document['collection'], document['code'])

                if xml:
                    global_xml.append(xml.find('article'))
            except Exception as exc:
                logger.exception('unhandled exception during validation of "%s"', document['code'])

        # Convertendo XML para texto
        try:
            textxml = etree.tostring(global_xml, encoding='utf-8', method='xml')
        except:
            pass

        if len(global_xml.findall('article')) == 0:
            continue

        xml_file = open(xml_file_name, 'w')
        xml_file.write(textxml)
        xml_file.close()

    #zipping files
    files = os.listdir('xml')
    zipped_file_name = tools.packing_zip(files)

    #sending to ftp.scielo.br
    tools.send_to_ftp(zipped_file_name,
                      ftp_host=FTP_HOST,
                      user=FTP_USER,
                      passwd=FTP_PASSWD)


def main():
    parser = argparse.ArgumentParser(
        description="Control the process of sending metadata to WoS")

    parser.add_argument(
        '-t',
        '--task',
        default='add',
        choices=['add', 'update'],
        help='Task that will be executed.'
    )

    parser.add_argument(
        '-c',
        '--clean_garbage',
        action='store_true',
        default=False,
        help='Remove processed files from FTP.'
    )

    parser.add_argument(
        '--logging_file',
        '-o',
        default='/var/log/exportsci/export_sci.log',
        help='Full path to the log file'
    )

    parser.add_argument(
        '--logging_level',
        '-l',
        default='DEBUG',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logggin level'
    )

    args = parser.parse_args()

    _config_logging(args.logging_level, args.logging_file)

    run(task=str(args.task), clean_garbage=bool(args.clean_garbage))
