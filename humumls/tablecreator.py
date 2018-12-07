"""Create a mongoDB from the UMLS Rich Release Format files."""
import os
import langid
from io import open
import re

from pymongo import MongoClient
from collections import defaultdict
from pymongo.errors import CollectionInvalid
from tqdm import tqdm


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
             preprocessor=lambda x: x,
             overwrite=False):
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
        {LANGDICT[l.upper()] for l in languages}
    except KeyError:
        raise KeyError("Not all languages you passed are valid.")
    # First create necessary paths, to fail early.
    try:
        collection = db.create_collection("term")
        terms = _create_terms(pathtometadir, languages)
        collection.insert_many(terms)
        del(terms)
    except CollectionInvalid:
        if overwrite:
            db.drop_collection("term")
            collection = db.create_collection("term")
            terms = _create_terms(pathtometadir, languages)
            collection.insert_many(terms)
            del(terms)
        else:
            print("term already exists, not overwriting.")

    try:
        collection = db.create_collection("string")
        strings = _create_strings(pathtometadir, languages)
        collection.insert_many(strings)
        del(strings)
    except CollectionInvalid:
        if overwrite:
            db.drop_collection("string")
            collection = db.create_collection("string")
            strings = _create_strings(pathtometadir, languages)
            collection.insert_many(strings)
            del(strings)
        else:
            print("string already exists, not overwriting.")

    try:
        collection = db.create_collection("concept")
        concepts = _create_concepts(pathtometadir,
                                    process_definitions,
                                    process_relations,
                                    process_semantic_types,
                                    languages,
                                    preprocessor)
        collection.insert_many(concepts)
        del(concepts)
    except CollectionInvalid:
        if overwrite:
            collection = db.create_collection("concept")
            concepts = _create_concepts(pathtometadir,
                                        process_definitions,
                                        process_relations,
                                        process_semantic_types,
                                        languages,
                                        preprocessor)
            collection.insert_many(concepts)
            del(concepts)
        else:
            print("concept already exists, not overwriting.")

    return db


def _create_concepts(path,
                     process_definitions,
                     process_relations,
                     process_semantic_types,
                     languages,
                     preprocessor):
    """
    Read MRCONSO for concepts.

    Parameters
    ----------
    path : string
        The path to the folder containing the MRCONSO file.
    process_definitions : bool
        Whether to process MRDEF, and add definitions to the database.
    process_relations : bool
        Whether to process MRREL, and add relations to the database.
    process_semantic_types : bool
        Whether to process MRSTY, and add semantic types to the database.
    languages : list of str
        The languages to use.
    preprocessor : function
        A function which preprocesses the data.

    Returns
    -------
    concepts : dict
        Dictionary of concept data, to be added to the database.

    """
    concepts = defaultdict(dict)

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

        c = concepts[cui]

        c["_id"] = cui

        if split[2] == "P":
            c["preferred"] = lui

        try:
            c["lui"].add(lui)
        except KeyError:
            c["lui"] = set([lui])
        try:
            c["sui"].add(sui)
        except KeyError:
            c["sui"] = set([sui])

    if process_definitions:
        concepts = process_mrdef(path, concepts, languages, preprocessor)
    if process_relations:
        concepts = process_mrrel(path, concepts)
    if process_semantic_types:
        concepts = process_mrsty(path, concepts)

    for v in concepts.values():
        try:
            v['lui'] = list(v['lui'])
        except KeyError:
            pass
        try:
            v['sui'] = list(v['sui'])
        except KeyError:
            pass

    return list(concepts.values())


def _create_terms(path, languages):
    """Read MRCONSO for terms."""
    terms = defaultdict(dict)

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

        t = terms[lui]

        t["_id"] = lui
        try:
            t["cui"].append(cui)
        except KeyError:
            t["cui"] = [cui]
        try:
            t["sui"].append(sui)
        except KeyError:
            t["sui"] = [sui]

    for v in terms.values():
        t["sui"] = list(t["sui"])
        t["cui"] = list(t["cui"])

    return list(terms.values())


def _create_strings(path, languages):
    """Read MRCONSO for strings."""
    strings = defaultdict(dict)

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
        tokenized = " ".join(PUNCT.sub(" ", string).split())

        s = strings[sui]

        s["_id"] = sui
        s["string"] = string
        s["lower"] = string.lower()
        s["tokenized"] = tokenized
        s["lang"] = split[1]
        s["numwords"] = len(string.split())
        s["numwordslower"] = len(tokenized.split())
        s["lui"] = lui
        try:
            s["cui"].add(cui)
        except KeyError:
            s["cui"] = set([cui])

    for v in strings.values():
        v['cui'] = list(v['cui'])

    return list(strings.values())


def process_mrrel(path, concepts):
    """
    Read the relations from MRREL.RRF, and add them to concepts.

    Because bidirectional relations in UMLS occur for both directions,
    only the direction which occurs in the UMLS is added.
    """
    mrrelpath = os.path.join(path, "MRREL.RRF")

    for idx, _ in enumerate(open(mrrelpath)):
        pass

    num_lines = idx

    print("Reading MRREL.RRF for relations.")
    for record in tqdm(open(mrrelpath), total=num_lines):

        split = record.strip().split("|")

        cui = split[4]
        dest = split[0]

        # provide dictionary mapping for REL
        rel = RELATIONMAPPING[split[3]]

        c = concepts[cui]
        c["rel"] = c.get("rel", {})

        try:
            concepts[cui]["rel"][rel].add(dest)
        except KeyError:
            concepts[cui]["rel"][rel] = set([dest])

    for v in concepts.values():
        try:
            for reltype in v["rel"]:
                v["rel"][reltype] = list(v["rel"][reltype])
        except KeyError:
            pass

    return concepts


def process_mrdef(path,
                  concepts,
                  languages,
                  preprocessor):
    """
    Read definitions from MRDEF.RRF.

    We append this to the intermediate dictionary (concepts), not the DB
    because this is way faster.

    Parameters
    ----------
    path : string
        The path to the META dir.
    concepts : dict
        The dictionary of concepts to which to add the definitions.
    languages : list of str
        The languages to use.
    preprocessor : function
        Function that preprocesses the defintions. Should be a function which
        takes a string as input and returns a string.

    Returns
    -------
    concepts : dict
        The updated concept dictionary with added definitions.

    """
    isolanguages = {LANGDICT[l.upper()] for l in languages}
    print(isolanguages)

    mrdefpath = os.path.join(path, "MRDEF.RRF")

    for idx, _ in enumerate(open(mrdefpath)):
        pass

    num_lines = idx

    print("Reading MRDEF.RRF for definitions.")
    for record in tqdm(open(mrdefpath), total=num_lines):
        split = record.strip().split("|")

        cui = split[0]
        c = concepts[cui]
        definition = split[5]

        # Detect language -> UMLS does not take into account language
        # in MRDEF.
        lang, _ = langid.classify(definition)
        if lang not in isolanguages:
            continue

        # Tokenize the definition.
        if preprocessor:
            definition = preprocessor(definition)
        try:
            c["definition"].append(definition)
        except KeyError:
            c["definition"] = [definition]

    return concepts


def process_mrsty(path, concepts):
    """Read semantic types from MRSTY.RRF."""
    mrstypath = os.path.join(path, "MRSTY.RRF")

    for idx, _ in enumerate(open(mrstypath)):
        pass

    num_lines = idx

    print("Reading MRSTY.RRF for semantic types.")
    for record in tqdm(open(mrstypath), total=num_lines):
        split = record.strip().split("|")

        cui = split[0]
        if cui not in concepts:
            continue
        c = concepts[cui]
        semantic_type = split[2]
        try:
            c = c["semtype"].append(semantic_type)
        except KeyError:
            c["semtype"] = [semantic_type]

    return concepts
