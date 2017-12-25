"""Example of aggregate queries."""
from collections import defaultdict
from itertools import chain

from humumls.connection import Connection
from humumls.table import String, Term, Concept


class Aggregator(object):
    """Example class of Aggregate queries using different UMLS tables."""

    def __init__(self, dbname="umls", hostname="localhost", port=27017):
        """Init method."""
        self._connection = Connection(dbname, hostname, port)

        self.string = String(self._connection)
        self.term = Term(self._connection)
        self.concept = Concept(self._connection)

    def concepts_string(self, string):
        """
        Get all concept that correspond to a string.

        Returns an empty list of strings which are not in the database.

        Parameters
        ----------
        string : str
            The string for which to search concepts

        Returns
        -------
        concepts : list
            A list of concepts, represented as dictionaries.

        """
        concepts = self.string.concept_id(string)
        if not concepts:
            return []

        return list(self.concept.bunch(concepts))

    def definitions(self, string):
        """
        Get all definitions given a string.

        First retrieves all concepts which are attached to a given string,
        and then retrieves all definitions attached to these concepts.

        Parameters
        ----------
        string : str
             The string for which to search definitions.

        Returns
        -------
        definitions : list
            A list of dictionaries containing the definitions.

        """
        string_obj = self.string.retrieve_one({"string": string},
                                              {"_id": 0, "cui": 1})
        if not string_obj:
            return []

        # Get the definitions.
        return self.concept.bunch_definitions(string_obj["cui"])

    def definitions_terms(self, string, relations=()):
        """
        Get all definitions + preferred terms for a given string.

        Parameters
        ----------
        string : str
            The string for which to retrieve the concepts and preferred terms.
        include_synonyms : list, optional, default ()
            The types of relations to include. For list of relations, check
            the README.

        Returns
        -------
        concepts : list
            A dictionary of concepts with the strings that refer to that
            concept.

        """
        cids = self.string.concept_id(string)
        if not cids:
            return []
        return self.definitions_terms_cid(cids, relations)

    def definitions_terms_cui(self,
                              cuis,
                              include_synonyms=(),
                              include_term=True):
        """Get the definitions and terms for concepts given their CUIs."""
        filt = {"_id": 1, "definition": 1, "preferred": 1, "rel": 1}
        concepts = self.concept.bunch(cuis, filt=filt)

        output = defaultdict(set)

        # Can be faster.
        for c in concepts:
            try:
                output[c["_id"]].update(c["definition"])
            except KeyError:
                pass

            if include_synonyms:

                for syn in include_synonyms:
                    try:
                        synonyms = self.definitions_terms_cui(c["rel"][syn],
                                                              (),
                                                              include_term)
                        output[c["_id"]].update(chain.from_iterable(synonyms))
                    except KeyError:
                        pass

            if include_term:
                term = self.term[c["preferred"]]
                output[c["_id"]].update(self.string.surface(term["string"]))

            output[c["_id"]] = list(output[c["_id"]])

        return output
