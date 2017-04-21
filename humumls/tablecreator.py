import os
import time
import logging
import langid
import io
import re

from pymongo import MongoClient
from collections import defaultdict


class TableCreator:
    """A class for creating the MongoDB form the Rich Release Format (RRF) files that come with the UMLS DB."""

    def __init__(self, languages, pathtometadir, dbname="umls", host="localhost", port=27017, tokenizer=lambda x: x):
        """
        Creates a TableCreator instance which will operate on the Rich Release Format (RRF) files which come with
        the UMLS database.

        The class is designed to directly operate on the RRF files, and doesn't require any formatting.
        It will create a mongoDB Database with the pre-specified name, host and port.

        The goal of this conversion step is NOT to conform closely to UMLS standards, but to provide a document-level
        alternative to the databases provided by other packages.

        :param languages: The languages to take into account. These must be given in the UMLS format, e.g. ENG for
        English, DUT for Dutch. See the _transformlangs_to_iso function for the languages present in UMLS.
        :param pathtometadir: The path to the /META directory which contains the RRF files. The only files that need
        to be present for this to work are the MRCONSO, MRDEF and MRREL files.
        :param dbname: The name of your DB. Defaults to UMLS.
        :param host: The hostname. defaults to localhost.
        :param port: The port on which your MongoDB runs. Default is 27107.
        :param tokenizer: The tokenize function to use for the definitions. If you don't want tokenization, we will
        tokenize using the identity function, e.g. tokenizer = lambda x: x.
        :return: None
        """
        self.client = MongoClient(host=host, port=port)
        self.db = self.client.get_database(dbname)
        self.path = pathtometadir
        self.punct = re.compile("\W")

        # Standard UMLS relation names are opaque.
        self.relationmapping = {"PAR": 'parent',
                                "CHD": 'child',
                                "RB": 'broader',
                                "RN": 'narrower',
                                "SY": 'synonym',
                                "RO": 'other',
                                "RL": 'similar',
                                "RQ": 'related',
                                "SIB": 'sibling',
                                "AQ": 'qualifier',
                                "QB": 'qualifies',
                                "RU": 'unspecified',
                                "XR": 'notrelated'}

        # Remove any duplicates
        self.languages = set(languages)

        # Transform non-standard language codings to ISO.
        self._isolanguages = self._transformlangs_to_iso(self.languages)
        self._tokenizer = tokenizer

        self.forbidden = set()

    @staticmethod
    def _transformlangs_to_iso(languages):
        """
        Transforms a list of languages, as defined in UMLS, to their ISO-compliant language names.
        Used in language identification.

        :param languages: A list of languages.
        :return: A list of ISO-compliant languages.
        """
        langdict = {'ENG': 'en',
                    'BAQ': 'eu',
                    'CHI': 'zh',
                    'CZE': 'cz',
                    'DAN': 'dk',
                    'DUT': 'nl',
                    'EST': 'et',
                    'FIN': 'fi',
                    'FRE': 'fr',
                    'GER': 'de',
                    'GRE': 'gr',
                    'HEB': 'he',
                    'HUN': 'hu',
                    'ITA': 'it',
                    'JPN': 'jp',
                    'KOR': 'ko',
                    'LAV': 'lv',
                    'NOR': 'no',
                    'POL': 'pl',
                    'POR': 'po',
                    'RUS': 'ru',
                    'SPA': 'sp',
                    'SWE': 'sw',
                    'TUR': 'tr'}

        return {langdict[l] for l in languages}

    def process(self, verbose=True, process_relations=True, process_definitions=True, overwrite=False):
        """
        Processes the UMLS files found at the pre-specified location.

        :param verbose: if true, prints status information to stdout.
        :param process_relations: whether to process relation files
        :param process_definitions: whether to process the definition files.
        :param overwrite: whether to overwrite.
        """
        if verbose:
            logging.basicConfig(level=logging.INFO)

        self._create_terms(overwrite)
        self._create_strings(overwrite)
        self._create_concepts(overwrite, process_relations, process_definitions)

        # Create extra index.
        self.db.string.create_index('string', unique=True)

        logging.info("done")

    def _create_concepts(self, overwrite, relations, definitions):
        """
        Reads MRCONSO for concepts. Stores everything in an intermediate dictionary, which requires
        several gigabytes of memory.

        :param overwrite: whether to overwrite.
        :param relations: whether to read relations.
        :param definitions: whether to read definitions.
        :return: None
        """
        collection = self.db.concept

        if collection.count() > 0 and not overwrite:
            logging.info("Skipped {0}".format(collection.name))
            return

        store = defaultdict(dict)

        start = time.time()

        for idx, record in enumerate(open(os.path.join(self.path, "MRCONSO.RRF"))):

            if idx % 100000 == 0:
                logging.info("{0} concepts in {1:.2f} seconds".format(idx, time.time() - start))

            split = record.strip().split("|")

            if split[1] not in self.languages:
                continue

            if split[5] in self.forbidden:
                continue

            pk = split[0]
            store[pk]["_id"] = pk

            if split[2] == "P":
                store[pk]["preferred"] = split[3]

            try:
                store[pk]["term"].add(split[3])
            except KeyError:
                store[pk]["term"] = {split[3]}

            try:
                store[pk]["string"].add(split[5])
            except KeyError:
                store[pk]["string"] = {split[5]}

        newtime = time.time()

        # Convert the items with sets to lists because of BSON.
        for key, item in store.items():
            store[key]["term"] = list(item["term"])
            store[key]["string"] = list(item["string"])
            store[key]["rel"] = {}

        logging.info("Conversion to BSON types took {0:.2f} seconds".format(time.time() - newtime))

        if definitions:
            store = self._mrdef(store)
        if relations:
            store = self._mrrel(store)

        newtime = time.time()
        store = {k: v for k, v in store.items() if "string" in v}
        logging.info("Converting store took {0:.2f} seconds".format(time.time() - newtime))

        newtime = time.time()

        collection.insert_many(store.values())
        logging.info("Inserting {0} items into {1} took {2:.2f} seconds".format(len(store),
                     collection.name,
                     time.time() - newtime))

    def _create_strings(self, overwrite):
        """
        Creates the string table by reading MRCONSO.RRF.

        Note that this throws away all words whose STR is over 1000 bytes long because of BSON constraints. For the
        2015 version of UMLS, this entails that we throw away 567 long strings, but these are all noisy.

        :param overwrite: whether to overwrite
        :return: the table.
        """
        collection = self.db.string

        if collection.count() > 0 and not overwrite:
            logging.info("Skipped {0}".format(collection.name))
            return

        store = defaultdict(dict)

        start = time.time()

        for idx, record in enumerate(io.open(os.path.join(self.path, "MRCONSO.RRF"), encoding='utf-8')):

            if idx % 100000 == 0:
                logging.info("{0} strings in {1} seconds".format(idx, time.time() - start))

            split = record.strip().split("|")

            if split[1] not in self.languages:
                continue

            pk = split[5]
            string = split[14]

            # Check BSON length
            if len(split[14].encode("utf-8")) >= 1000:
                logging.info(split[14])
                self.forbidden.add(pk)
                continue

            # Create lexical representation.
            lowerwords = " ".join(self.punct.sub(" ", string).lower().split())

            store[pk]["_id"] = pk
            store[pk]["string"] = string
            store[pk]["lower"] = lowerwords
            store[pk]["lang"] = split[1]
            store[pk]["numwords"] = len(string.split())
            store[pk]["numwordslower"] = len(lowerwords.split())
            store[pk]["term"] = split[3]

            try:
                store[pk]["concept"].add(split[0])
            except KeyError:
                store[pk]["concept"] = {split[0]}

        newtime = time.time()

        # Convert the items with sets to lists because of BSON.
        for key, item in store.items():
            store[key]["concept"] = list(item["concept"])

        logging.info("Conversion to BSON types took {0:.2f} seconds".format(time.time() - newtime))

        newtime = time.time()

        collection.insert_many(store.values())
        logging.info("Inserting {0} items into {1} took {2:.2f} seconds".format(len(store),
                     collection.name,
                     time.time() - newtime))

        logging.info("Threw away {0} items".format(len(self.forbidden)))

        return store

    def _create_terms(self, overwrite):
        """
        Creates the Term collection from the MRCONSO.RRF file.

        :param overwrite: whether to overwrite the current collection.
        :return: None
        """

        collection = self.db.term

        if collection.count() > 0 and not overwrite:
            logging.info("Skipped {0}".format(collection.name))
            return

        store = defaultdict(dict)

        start = time.time()

        for idx, record in enumerate(open(os.path.join(self.path, "MRCONSO.RRF"))):

            if idx % 100000 == 0:
                logging.info("{0} terms in {1:.2f} seconds".format(idx, time.time() - start))

            split = record.strip().split("|")

            if split[1] not in self.languages:
                continue

            if split[5] in self.forbidden:
                continue

            # Get the key from the table name.
            pk = split[3]

            store[pk]["_id"] = pk

            # For each non-primary key, add the item.
            try:
                store[pk]["concept"].add(split[0])
            except KeyError:
                store[pk]["concept"] = {split[0]}

            # For each non-primary key, add the item.
            try:
                store[pk]["string"].add(split[5])
            except KeyError:
                store[pk]["string"] = {split[5]}

        newtime = time.time()

        # Convert the items with sets to lists because of BSON.
        for key, item in store.items():
            store[key]["concept"] = list(item["concept"])
            store[key]["string"] = list(item["string"])

        logging.info("Conversion to BSON types took {0:.2f} seconds".format(time.time() - newtime))

        newtime = time.time()
        collection.insert_many(store.values())
        logging.info("Inserting {0} items into {1} took {2:.2f} seconds".format(len(store),
                     collection.name,
                     time.time() - newtime))

        return store

    def _mrrel(self, store):
        """
        Reads the relations from MRREL.RRF, and appends them to the corresponding items in the store.
        Because bidirectional relations in UMLS occur for both directions, only the direction which occurs in the
        UMLS is added.

        :param store: The store to which to add.
        :return: the updated store.
        """
        start = time.time()

        for idx, record in enumerate(open(os.path.join(self.path, "MRREL.RRF"))):

            if idx % 100000 == 0:
                logging.info("{0} relations in {1:.2f} seconds".format(idx, time.time() - start))

            split = record.strip().split("|")

            source = split[4]
            dest = split[0]

            # provide dictionary mapping for REL
            rel = self.relationmapping[split[3]]

            try:
                store[source]["rel"][rel].add(dest)
            except KeyError:
                store[source]["rel"] = defaultdict(set)

        for key in store.keys():
            for k, v in store[key]["rel"].items():
                store[key]["rel"][k] = list(v)

        return store

    def _mrdef(self, store):
        """
        Reads the definitions from MRDEF.RRF and appends them to the corresponding items in the store.

        It is appended to the store, and not to the DB because this is way faster.

        :param store: The store to which to append.
        :return: the updated store.
        """
        start = time.time()

        for idx, record in enumerate(open(os.path.join(self.path, "MRDEF.RRF"))):

            if idx % 100000 == 0 and idx > 0:
                logging.info("{0} definitions in {1:.2f} seconds.".format(idx, time.time() - start))

            split = record.strip().split("|")

            pk = split[0]

            definition = split[5]

            # Detect language -> UMLS does not take into account language in MRDEF.
            lang, _ = langid.classify(definition)

            if lang not in self._isolanguages:
                continue

            # Tokenize the definition and remove any HTML.
            if self._tokenizer:
                definition = self._tokenizer(definition)

            try:
                store[pk]["definition"].add(definition)
            except KeyError:
                store[pk]["definition"] = {definition}

        for k in store.keys():
            try:
                store[k]["definition"] = list(store[k]["definition"])
            except KeyError:
                continue

        return store