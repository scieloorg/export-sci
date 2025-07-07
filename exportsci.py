# coding: utf-8

from datetime import datetime
import os
import argparse
import logging
import shutil

import tools
import utils
from lxml import etree

from utils import earlier_yyyymmdd


logger = logging.getLogger(__name__)
config = utils.Configuration.from_env()
settings = dict(config.items())["main:exportsci"]

FTP_HOST = settings["ftp_host"]
FTP_USER = settings["ftp_user"]
FTP_PASSWD = settings["ftp_passwd"]
MONGODB_HOST = settings["mongodb_host"]
MONGODB_SLAVEOK = bool(settings["mongodb_slaveok"])
WOS_COLLECTIONS_ALLOWED = settings["wos_collections_allowed"].strip().split(",")


def _config_logging(logging_level="INFO", logging_file=None):

    allowed_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    logger.setLevel(allowed_levels.get(logging_level, "INFO"))

    if logging_file:
        hl = logging.FileHandler(logging_file, mode="a")
    else:
        hl = logging.StreamHandler()

    hl.setFormatter(formatter)
    hl.setLevel(allowed_levels.get(logging_level, "INFO"))

    logger.addHandler(hl)

    return logger


def _get_collection_issns(collection):
    collection_issns = []
    path = f"controller/{collection}.txt"
    if not os.path.isfile(path):
        template_path = f"controller.template/{collection}.txt"
        if not os.path.isfile(template_path):
            logger.error(f"Not found {path}")
            logger.error(f"Not found {template_path}")
            exit()
        shutil.copyfile(template_path, path)

    with open(f"controller/{collection}.txt", "r") as fp:
        collection_issns = [item.strip() for item in fp.realines()]
    if not collection_issns:
        logger.info(f"No issns ({collection})")
        exit()
    return collection_issns


def run(collection, task="add", clean_garbage=False, normalize=True):
    required_dirs = ["controller", "reports", "xml"]
    working_dir = os.listdir(".")
    logger.debug("Validating working directory %s" % working_dir)
    for d in required_dirs:
        if d not in working_dir:
            logger.error(f"Working dir does not have {d} directory")
            exit()

    if clean_garbage:
        logger.debug("Removing previous XML files")
        os.system("rm -f xml/*.xml")
        logger.debug("Removing previous zip files")
        os.system("rm -f *.zip")
        logger.debug("Removing previous error report files")
        os.system("rm -f report/*errors.txt")

    if task == "update":
        logger.debug("Loading toupdate.txt ISSN's file from FTP controller directory")
        tools.get_to_update_file_from_ftp(
            ftp_host=FTP_HOST, user=FTP_USER, passwd=FTP_PASSWD
        )
        issns = tools.load_journals_list(journals_file="controller/toupdate.txt")
    elif task == "add":
        logger.debug("Loading keepinto.txt ISSN's file from FTP controller directory")
        tools.get_keep_into_file_from_ftp(
            ftp_host=FTP_HOST, user=FTP_USER, passwd=FTP_PASSWD
        )
        issns = tools.load_journals_list(journals_file="controller/keepinto.txt")

    collection_issns = _get_collection_issns(collection)
    valid_issns = set(issns) & set(collection_issns)
    if not valid_issns:
        logger.error(f"No valid issns to process")
        exit()

    # Setup a connection to SciELO Network Collection
    logger.debug("Connecting to mongodb with DataHandler thru %s" % (MONGODB_HOST))
    dh = tools.DataHandler(MONGODB_HOST)
    collections = dh.load_collections_metadata()

    # logger.debug("Remove previous inbound files")
    # tools.remove_previous_unbound_files_from_ftp(ftp_host=FTP_HOST,
    #                              user=FTP_USER,
    #                              passwd=FTP_PASSWD)

    # logger.debug("Syncing XML's status according to WoS validated files")
    # tools.get_sync_file_from_ftp(ftp_host=FTP_HOST,
    #                              user=FTP_USER,
    #                              passwd=FTP_PASSWD)

    # dh.sync_sent_documents(remove_origin=clean_garbage)

    # logger.debug("Creating file with a list of documents to be removed from WoS")
    # tools.get_take_off_files_from_ftp(ftp_host=FTP_HOST,
    #                                   user=FTP_USER,
    #                                   passwd=FTP_PASSWD,
    #                                   remove_origin=clean_garbage)

    # ids_to_remove = dh.load_pids_list_to_be_removed()

    # tools.send_take_off_files_to_ftp(ftp_host=FTP_HOST,
    #                                  user=FTP_USER,
    #                                  passwd=FTP_PASSWD,
    #                                  remove_origin=clean_garbage)

    logger.debug("Defining document types elegible to send to SCI")
    dh.set_elegible_document_types()

    xml_validator = tools.XMLValidator()
    now = datetime.now().isoformat()[0:10]

    # Loading XML files
    for issn in valid_issns:

        # if issn in ids_to_remove:
        #     logger.debug(
        #         "Issn {0} is available in the takeoff and keepinto file. For now this ISSN was ignored, and will not be send to WoS until it is removed from the takeoff file.".format(
        #             issn
        #         )
        #     )
        #     continue

        folders = [
            "xml",
            collection,
            issn,
        ]
        issn_xml_path = "/".join(folders)
        if not os.path.exists(issn_xml_path):
            os.makedirs(issn_xml_path)

        proc_date_ctrl = ProcessingDateController(issn)

        if task == "update":
            try:
                documents = dh.sent_to_wos_with_proc_date(
                    issn,
                    proc_date_ctrl.from_date,
                )
            except:
                documents = None
            if documents is None:
                documents = dh.sent_to_wos(issn)

            xml_file_name = "{}/SciELO_COR_{}_{}.xml".format(issn_xml_path, issn, now)
            pids_filename = "{}/pids_COR_{}_{}.txt".format(issn_xml_path, issn, now)
        elif task == "add":
            try:
                documents = dh.not_sent_with_proc_date(
                    WOS_COLLECTIONS_ALLOWED,
                    issn,
                    publication_year=2002,
                )
            except:
                documents = None
            if documents is None:
                documents = dh.not_sent(
                    WOS_COLLECTIONS_ALLOWED, issn, publication_year=2002
                )
            xml_file_name = "{}/SciELO_{}_{}.xml".format(issn_xml_path, issn, now)
            pids_filename = "{}/pids_{}_{}.txt".format(issn_xml_path, issn, now)

        nsmap = {
            "xml": "http://www.w3.org/XML/1998/namespace",
            "xlink": "http://www.w3.org/1999/xlink",
        }
        global_xml = etree.Element("articles", nsmap=nsmap)
        global_xml.set("dtd-version", "1.12")
        global_xml.set(
            "{http://www.w3.org/2001/XMLSchema-instance}noNamespaceSchemaLocation",
            "Clarivate_publishing_1.12.xsd",
        )

        pids = []
        for total, current, document in documents:
            logger.info("{} - total xmls: {}".format(issn, total))

            try:
                # skip ahead documents
                if (
                    "v32" in document["article"]
                    and "ahead" in document["article"]["v32"][0]["_"].lower()
                ):
                    continue

                xml = xml_validator.validate_xml(
                    document["collection"], document["code"]
                )

                if xml:
                    global_xml.append(xml.find("article"))
                    pids.append(document["code"])
            except Exception as exc:
                logger.exception(
                    'unhandled exception during validation of "%s"', document["code"]
                )

        if not pids:
            logger.error("No valid xml")
            continue

        # Convertendo XML para texto
        logger.info("{} - total valid xmls: {}".format(issn, len(pids)))
        try:
            textxml = etree.tostring(global_xml, encoding="utf-8", method="xml")
        except Exception as exc:
            logger.error("Unable to generate XML {}: {}".format(xml_file_name, exc))
            continue
        else:
            with open(xml_file_name, "w") as fp:
                xml_file.write(textxml)

        try:
            # zipping files
            now = datetime.now().isoformat()[0:10]
            zip_filename = "scielo_{}_{}.zip".format(now, issn)
            zipped_file_name = tools.packing_zip(
                xml_file_name, None, None, zip_filename
            )
        except Exception as exc:
            logger.error("Unable to generate zip for {}: {}".format(xml_file_name, exc))
            continue

        try:
            # sending to ftp.scielo.br
            tools.send_to_ftp(
                zipped_file_name, ftp_host=FTP_HOST, user=FTP_USER, passwd=FTP_PASSWD
            )
        except Exception as exc:
            logger.error("Unable to ftp {}: {}".format(zipped_file_name, exc))
        else:
            dh.mark_documents_as_sent_to_wos(pids)
            with open(pids_filename, "w") as fp:
                fp.write("\n".join(pids))
            shutil.move(zipped_file_name, "zips")


def skip_because_of_processing_date(proc_date_ctrl, document):
    try:
        processing_date = _get_processing_date(document)
        if processing_date and processing_date < proc_date_ctrl.from_date:
            logger.info(
                "Skipping because of the processing date: %s < %s"
                % (processing_date, proc_date_ctrl.from_date)
            )
            return True
        proc_date_ctrl.save_most_recent_processing_date(processing_date)
    except Exception as e:
        logger.exception("Processing date [%s]: %s" % (document["code"], e))


def _get_processing_date(document):
    try:
        return document["article"].get("v91", [{"_": ""}])[0]["_"]
    except (KeyError, IndexError, ValueError, TypeError):
        return None


class ProcessingDateController:

    def __init__(self, issn, safer_days=None):
        self._issn = issn
        self._from_date = None
        self._safer_days = safer_days or 10
        self._file_path = "processing_dates/{}.txt".format(self._issn)
        _dirname = os.path.dirname(self._file_path)
        if not os.path.isdir(_dirname):
            os.makedirs(_dirname)

    @property
    def from_date(self):
        if self._from_date is None:
            most_recent = self._read_most_recent_processing_date()
            if most_recent:
                self._from_date = earlier_yyyymmdd(most_recent, days=self._safer_days)
            else:
                self._from_date = earlier_yyyymmdd()
        return self._from_date

    def _read_most_recent_processing_date(self):
        try:
            with open(self._file_path, "r") as fp:
                return fp.read()
        except:
            return None

    def save_most_recent_processing_date(self, processing_date):
        try:
            if processing_date and processing_date > self.from_date:
                with open(self._file_path, "w") as fp:
                    fp.write(processing_date)
        except:
            return None


def main():
    here = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(here, "VERSION")) as f:
        VERSION = f.read()
        print("Export SciELOCI %s" % VERSION)

    parser = argparse.ArgumentParser(
        description="Control the process of sending metadata to WoS"
    )

    parser.add_argument("collection", help="Collection acron")

    parser.add_argument(
        "-t",
        "--task",
        default="add",
        choices=["add", "update"],
        help="Task that will be executed.",
    )

    parser.add_argument(
        "-c",
        "--clean_garbage",
        action="store_true",
        default=False,
        help="Remove processed files from FTP.",
    )

    parser.add_argument(
        "--logging_file",
        "-o",
        default="/var/log/exportsci/export_sci.log",
        help="Full path to the log file",
    )

    parser.add_argument(
        "--logging_level",
        "-l",
        default="DEBUG",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logggin level",
    )

    args = parser.parse_args()

    _config_logging(args.logging_level, args.logging_file)
    logging.debug("Export SciELOCI %s" % VERSION)
    run(
        collection=args.collection,
        task=str(args.task),
        clean_garbage=bool(args.clean_garbage),
    )
