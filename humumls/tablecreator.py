"""Create a mongoDB from the UMLS Rich Release Format files."""
import os
import langid
from io import open
import re

from pymongo import MongoClient
from collections import defaultdict
from pymongo import ASCENDING
from tqdm import tqdm


def mytype():
    """Type for the store dict."""
    return defaultdict(list)


PUNCT = re.compile("\W")

LANGDICT = {'ENG': 'en',
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

RELATIONMAPPING = {"PAR": 'parent',
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


def createdb(pathtometadir,
             languages=(),
             dbname="umls",
             host="localhost",
             port=27017,
             process_definitions=True,
             process_relations=True,
             process_semantic_types=True,
             preprocessor=lambda x: x):
    """
    Create a MongoDB instance from the RRF format in which UMLS is distributed.

    The class is designed to directly operate on the RRF files,
    and doesn't require any formatting.
    It will create a mongoDB Database with the pre-specified name,
    host and port.

    Parameters
    ----------
    languages : list of string
        The languages to extract from the UMLS database. These must be
        passed in UMLS format, not in an ISO-compliant standard.
        See LANGDICT for language key value mappings.
    pathtometadir : string
        The path to the META directory which contains the RRF files. The
        only files that Humumls needs are the MRCONSO, MRDEF and MRREL
        files.
    dbname : string, optional, default "umls"
        The name of the mongodb database you are about to create.
    host : string, optional, default "localhost"
        The name of your host.
    port : int
        The port on which your mongodb instance resides.
    preprocessor : function, optional, default identity
        The preprocessor you would like to use. This should be a function
        which takes a string and returns a string. An example of this is
        a tokenizer or a HTML stripper.

    """
    client = MongoClient(host=host, port=port)
    db = client.get_database(dbname)

    # Remove any duplicates
    languages = set(languages)

    # Transform non-standard language codings to ISO.
    try:
        {LANGDICT[l] for l in languages}
    except KeyError:
        raise KeyError("Not all languages you passed are valid.")
    # First create necessary paths, to fail early.
    terms = _create_terms(pathtometadir, languages)
    '''db.term.insert_many(terms.values())
    del(terms)'''
    strings = _create_strings(pathtometadir, languages)
    '''db.string.insert_many(strings.values())
    del(strings)'''

    concepts = _create_concepts(pathtometadir,
                                process_definitions,
                                process_relations,
                                process_semantic_types,
                                languages,
                                preprocessor)
    '''db.concept.insert_many(concepts.values())
    del(concepts)'''

    # Create extra index.
    db.string.create_index([("string", ASCENDING),
                            ("lower", ASCENDING),
                            ("lang", ASCENDING)], unique=True)

    return concepts, strings, terms


def _create_concepts(path,
                     process_definitions,
                     process_relations,
                     process_semantic_types,
                     languages,
                     preprocessor):
    """
    Read MRCONSO for concepts, strings, and terms.

    Parameters
    ----------
    path : string
        The path to the folder containing the MRCONSO file.
    process_definitions : bool
        Whether to process MRDEF, and add definitions to the database.
    process_relations : bool
        Whether to process MRREL, and add relations to the database.
    languages : list of str
        The languages to use.
    """
    concepts = defaultdict(mytype)

    mrcsonsopath = os.path.join(path, "MRCONSO.RRF")

    for idx, _ in enumerate(open(mrcsonsopath)):
        pass

    num_lines = idx

    print("Reading MRCONSO for concepts.")
    for record in tqdm(open(mrcsonsopath), total=num_lines):

        split = record.strip().split("|")

        if languages and split[1] not in languages:
            continue

        cui = split[0]
        sui = split[5]
        lui = split[3]

        concepts[cui]["_id"] = cui

        if split[2] == "P":
            concepts[cui]["preferred"] = lui

        concepts[cui]["lui"].append(lui)
        concepts[cui]["sui"].append(sui)

    if process_definitions:
        concepts = process_mrdef(path, concepts, languages, preprocessor)
    if process_relations:
        concepts = process_mrrel(path, concepts)
    if process_semantic_types:
        concepts = process_mrsty(path, concepts)

    return dict(concepts)

def _create_terms(path, languages):

    terms = defaultdict(mytype)

    mrcsonsopath = os.path.join(path, "MRCONSO.RRF")

    for idx, _ in enumerate(open(mrcsonsopath)):
        pass

    num_lines = idx

    print("Reading MRCONSO for terms.")
    for record in tqdm(open(mrcsonsopath), total=num_lines):

        split = record.strip().split("|")

        if languages and split[1] not in languages:
            continue

        cui = split[0]
        sui = split[5]
        lui = split[3]

        terms[lui]["_id"] = lui
        terms[lui]["cui"].append(cui)
        terms[lui]["sui"].append(sui)

    return terms


def _create_strings(path, languages):

    strings = defaultdict(mytype)

    mrcsonsopath = os.path.join(path, "MRCONSO.RRF")

    for idx, _ in enumerate(open(mrcsonsopath)):
        pass

    num_lines = idx

    print("Reading MRCONSO for strings.")
    for record in tqdm(open(mrcsonsopath), total=num_lines):

        split = record.strip().split("|")
        string = split[14]

        # Check BSON length
        byte_string = string.encode("utf-8")
        if len(byte_string) >= 1000:
            # Truncate 1000 bytes
            string = byte_string[:1000].decode('utf-8')

        if languages and split[1] not in languages:
            continue

        cui = split[0]
        sui = split[5]
        lui = split[3]

        # Create lexical representation.
        lowerwords = " ".join(PUNCT.sub(" ", string).lower().split())

        strings[sui]["_id"] = sui
        strings[sui]["string"] = string
        strings[sui]["lower"] = lowerwords
        strings[sui]["lang"] = split[1]
        strings[sui]["numwords"] = len(string.split())
        strings[sui]["numwordslower"] = len(lowerwords.split())
        strings[sui]["lui"] = lui
        strings[sui]["cui"].append(cui)

    return strings


def process_mrrel(path, concepts):
    """
    Reads the relations from MRREL.RRF, and appends them to the corresponding items in the store.
    Because bidirectional relations in UMLS occur for both directions, only the direction which occurs in the
    UMLS is added.

    :param store: The store to which to add.
    :return: the updated store.
    """
    mrrelpath = os.path.join(path, "MRREL.RRF")

    for idx, _ in enumerate(open(mrrelpath)):
        pass

    num_lines = idx

    for record in tqdm(open(mrrelpath), total=num_lines):

        split = record.strip().split("|")

        source = split[4]
        dest = split[0]

        # provide dictionary mapping for REL
        rel = RELATIONMAPPING[split[3]]

        try:
            concepts[source]["rel"][rel].append(dest)
        except TypeError:
            concepts[source]["rel"] = defaultdict(list)
            concepts[source]["rel"][rel].append(dest)

    return concepts


def process_mrdef(path,
                  concepts,
                  languages,
                  preprocessor):
    """
    Reads the definitions from MRDEF.RRF and appends them to the corresponding items in the store.

    It is appended to the store, and not to the DB because this is way faster.

    :param store: The store to which to append.
    :return: the updated store.
    """
    isolanguages = {LANGDICT[l] for l in languages}

    mrdefpath = os.path.join(path, "MRDEF.RRF")

    for idx, _ in enumerate(open(mrdefpath)):
        pass

    num_lines = idx

    for record in tqdm(open(mrdefpath), total=num_lines):
        split = record.strip().split("|")

        pk = split[0]

        definition = split[5]

        # Detect language -> UMLS does not take into account language
        # in MRDEF.
        lang, _ = langid.classify(definition)

        if lang not in isolanguages:
            continue

        # Tokenize the definition.
        if preprocessor:
            definition = preprocessor(definition)
        concepts[pk]["definition"].append(definition)

    for k in {k for k, v in concepts.items() if 'definition' in v}:
        try:
            concepts[k]["definition"] = list(concepts[k]["definition"])
        except KeyError:
            continue

    return concepts

def process_mrsty(path, concepts):

    mrstypath = os.path.join(path, "MRSTY.RRF")

    for idx, _ in enumerate(open(mrstypath)):
        pass

    num_lines = idx

    for record in tqdm(open(mrstypath), total=num_lines):
        split = record.strip().split("|")

        cui = split[0]

        semantic_type = split[2]
        concepts[cui]["semtype"].append(semantic_type)

    return concepts
