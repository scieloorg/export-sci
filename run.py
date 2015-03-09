from datetime import datetime
import os
import argparse
import logging

import tools
import config
from lxml import etree

from normalization import Normalization

def _config_logging(logging_level='INFO', logging_file=None):

    allowed_levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger = logging.getLogger('export_scl')
    logger.setLevel(allowed_levels.get(logging_level, 'INFO'))

    if logging_file:
        hl = logging.FileHandler(logging_file, mode='a')
    else:
        hl = logging.StreamHandler()

    hl.setFormatter(formatter)
    hl.setLevel(allowed_levels.get(logging_level, 'INFO'))

    logger.addHandler(hl)

    return logger


def main(task='add', clean_garbage=False, normalize=True):
    # Setup a connection to SciELO Network Collection
    coll_articles = tools.get_articles_collection(config.MONGODB_HOST,
                                                  config.MONGODB_PORT)
    coll_collections = tools.get_collections_collection(config.MONGODB_HOST,
                                                        config.MONGODB_PORT)

    now = datetime.now().isoformat()[0:10]
    index_issn = 0

    collections = tools.load_collections_metadata(coll_collections)

    if clean_garbage:
        logger.debug("Removing previous XML files")
        os.system('rm -f tmp/xml/*.xml')
        logger.debug("Removing previous zip files")
        os.system('rm -f tmp/*.zip')
        logger.debug("Removing previous error report files")
        os.system('rm -f report/*errors.txt')

    logger.debug("Including collections url to journals metadata")
    tools.include_collection_url_to_journals_metadata(coll_articles,
                                                      collections)

    logger.debug("Downloading doi file from ftp")
    tools.get_doi_file_from_ftp(ftp_host=config.FTP_HOST,
                                user=config.FTP_USER,
                                passwd=config.FTP_PASSWD)

    if normalize:
        logger.debug("Downloading normalized names for countries and institutions")
        logger.debug("Removing previous error report files")
        os.system('mv notfound* controller/')

        tools.get_normalization_files_from_ftp(ftp_host=config.FTP_HOST,
                                               user=config.FTP_USER,
                                               passwd=config.FTP_PASSWD)

        norm = Normalization(conversion_table='controller/normalized_country.csv',
                             mongodb_host=config.MONGODB_HOST,
                             mongodb_port=config.MONGODB_PORT)

        norm.bulk_data_fix(field='article.v70',
                           subfield='p')

        norm = Normalization(conversion_table='controller/normalized_institution.csv',
                             mongodb_host=config.MONGODB_HOST,
                             mongodb_port=config.MONGODB_PORT)

        norm.bulk_data_fix(field='article.v70')

        norm = Normalization(conversion_table='controller/normalized_country_by_institution.csv',
                             mongodb_host=config.MONGODB_HOST,
                             mongodb_port=config.MONGODB_PORT)

        norm.bulk_data_fix(field='article.v70',
                           write_into='p')

    if task == 'update':
        logger.debug("Loading toupdate.txt ISSN's file from FTP controller directory")
        tools.get_to_update_file_from_ftp(ftp_host=config.FTP_HOST,
                                          user=config.FTP_USER,
                                          passwd=config.FTP_PASSWD)

        issns = tools.load_journals_list(
            journals_file='controller/toupdate.txt')

    elif task == 'add':
        logger.debug("Loading keepinto.txt ISSN's file from FTP controller directory")
        tools.get_keep_into_file_from_ftp(ftp_host=config.FTP_HOST,
                                          user=config.FTP_USER,
                                          passwd=config.FTP_PASSWD)

        issns = tools.load_journals_list(
            journals_file='controller/keepinto.txt')

    logger.debug("Syncing XML's status according to WoS validated files")
    tools.get_sync_file_from_ftp(ftp_host=config.FTP_HOST,
                                 user=config.FTP_USER,
                                 passwd=config.FTP_PASSWD)

    tools.sync_validated_xml(coll_articles)

    logger.debug("Creating file with a list of documents to be removed from WoS")
    tools.get_take_off_files_from_ftp(ftp_host=config.FTP_HOST,
                                      user=config.FTP_USER,
                                      passwd=config.FTP_PASSWD,
                                      remove_origin=clean_garbage)

    ids_to_remove = tools.load_pids_list_to_be_removed(coll_articles)

    tools.send_take_off_files_to_ftp(ftp_host=config.FTP_HOST,
                                     user=config.FTP_USER,
                                     passwd=config.FTP_PASSWD,
                                     remove_origin=clean_garbage)

    logger.debug("Update doi numbers according to field 237")
    tools.load_doi_from_237(coll_articles)

    logger.debug("Update doi numbers according to text files")
    tools.load_doi_from_file(coll_articles)

    logger.debug("Defining document types elegible to send to SCI")
    tools.set_elegible_document_types(coll_articles)

    # Loading XML files
    for issn in issns:
        index_issn = index_issn + 1

        if issn in ids_to_remove:
            logger.debug("Issn {0} is available in the takeoff and keepinto file. For now this ISSN was ignored, and will not be send to WoS until it is removed from the takeoff file.".format(issn))
            continue

        if task == 'update':
            documents = tools.validated_wos(coll_articles, issn, publication_year=2002)
            xml_file_name = "tmp/xml/SciELO_COR_{0}_{1}.xml".format(now, issn)
        elif task == 'add':
            documents = tools.not_send(coll_articles, issn, publication_year=2002)
            xml_file_name = "tmp/xml/SciELO_{0}_{1}.xml".format(now, issn)

        if documents.count() == 0:
            continue

        logger.debug("validating {0} xml's for {1}".format(documents.count(), issn))
        logger.debug("Loading documents to be validated")

        if not os.path.exists('tmp/xml'):
            os.makedirs('tmp/xml')

        xml = ''
        xmlr = ''
        if not os.path.exists(xml_file_name):
            for document in documents:

                #skip ahead documents
                if 'v32' in document.data['article'] and 'ahead' in document.data['article']['v32'].lower():
                    continue

                xml = tools.validate_xml(coll_articles,
                                         document['code'],
                                         issn,
                                         api_host='articlemeta.scielo.org')
                if xml:
                    parser = etree.XMLParser(remove_blank_text=True)
                    root = etree.fromstring(xml, parser)
                    xmlr = xmlr + etree.tostring(root.getchildren()[0])

            if xml:
                xml_file = open(xml_file_name, 'w')
                xml_file.write('<articles xmlns:xlink="http://www.w3.org/1999/xlink" '\
                               'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '\
                               'xsi:noNamespaceSchemaLocation="ThomsonReuters_publishing_1.09.xsd" '\
                               'dtd-version="1.09">')
                xml_file.write(xmlr)
                xml_file.write("</articles>")
                xml_file.close()

        else:
            logger.debug("File {0} already exists".format(xml_file_name))

    #zipping files
    files = os.listdir('tmp/xml')
    zipped_file_name = tools.packing_zip(files)

    #sending to ftp.scielo.br
    tools.send_to_ftp(zipped_file_name,
                      ftp_host=config.FTP_HOST,
                      user=config.FTP_USER,
                      passwd=config.FTP_PASSWD)


if __name__ == "__main__":
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
        '-n',
        '--normalize',
        action='store_true',
        default=False,
        help='Run normalization processing.'
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

    logger = _config_logging(args.logging_level, args.logging_file)

    main(task=str(args.task),
         clean_garbage=bool(args.clean_garbage),
         normalize=bool(args.normalize))
