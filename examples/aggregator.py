from collections import defaultdict
from itertools import chain

from humumls.connection import Connection
from humumls.table import String, Term, Concept


class Aggregator(object):
    """Example class of Aggregate queries using different UMLS tables"""

    def __init__(self, dbname="umls", hostname="localhost", port=27017):
        """
        Initialize an aggregator of different tables.
        :param dbname: the name of the DB, default is UMLS
        :param hostname: the name of the host, default is localhost
        :param port: the port on which your mongodb runs, default is 27107
        :return: None
        """
        self._connection = Connection(dbname, hostname, port)

        self.string = String(self._connection)
        self.term = Term(self._connection)
        self.concept = Concept(self._connection)

    def concepts_string(self, string):
        """
        Get all concept objects given a string.
        :param string: the string for which to search concepts
        :return: a list of concepts.
        """
        concepts = self.string.concept_id(string)
        if not concepts:
            return []

        return list(self.concept.bunch(concepts))

    def definitions(self, string):
        """
        Get all definitions given a string.
        :param string: the string for which to search definitions.
        :return: a dictionary of concepts which contains the definition of that concept.
        """
        string_obj = self.string.retrieve_one({"string": string}, {"_id": 1, "cui": 1})
        if not string_obj:
            return []

        return self.concept.bunch_definitions(string_obj["cui"])

    def definitions_terms(self, string, include_synonyms=()):
        """
        Get all definitions + preferred terms for a given string. Useful for creating concept representations.
        :param string: the string for which to retrieve the concepts and preferred terms.
        :param include_synonyms: whether to include synonyms.
        :return: a dictionary of concepts with the strings that refer to that concept.
        """
        cids = self.string.concept_id(string)

        return self.definitions_terms_cid(cids, include_synonyms)

    def definitions_terms_cid(self, cids, include_synonyms=(), include_term=True):
        """
        Get all definitions from a cid.
        :param cids: a list of cids
        :param include_synonyms: The types of synonyms to include.
        :param include_term: whether to use the preferred term.
        :return: A list of definitions, grouped by concept.
        """
        concepts = self.concept.bunch(cids, filt={"_id": 1, "definition": 1, "preferred": 1, "rel": 1})

        output = defaultdict(set)

        for c in concepts:
            try:
                output[c["_id"]].update(c["definition"])
            except KeyError:
                pass

            if include_synonyms:

                for syn in include_synonyms:
                    try:
                        synonyms = self.definitions_terms_cid(c["rel"][syn], include_term=include_term, include_synonyms=()).values()
                        output[c["_id"]].update(chain.from_iterable(synonyms))
                    except KeyError:
                        pass

            if include_term:
                term = self.term[c["preferred"]]
                output[c["_id"]].update(self.string.surface(term["string"]))

            output[c["_id"]] = list(output[c["_id"]])

        return output
