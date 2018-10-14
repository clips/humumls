"""Example of relation queries."""
from itertools import chain

from humumls.connection import Connection
from humumls import String, Term, Concept


class Relations(object):
    """Example class of Aggregate queries using different UMLS tables."""

    def __init__(self, dbname="umls", hostname="localhost", port=27017):
        """Init method."""
        self._connection = Connection(dbname, hostname, port)

        self.string = String(self._connection)
        self.term = Term(self._connection)
        self.concept = Concept(self._connection)

    def get_child_words(self, string):
        """Get all words which are children of a word."""
        cuis = self.string.cuis(string)
        children = [x['rel']['child'] for x in self.concept.bunch(cuis)]
        children = list(chain.from_iterable(children))

        children_strings = [x['string'] for x in self.concept.bunch(children)]
        return list(chain.from_iterable(children_strings))

    def get_all_children(self, cui):
        """Recursively get all children of a cui."""
        cuis = [cui]
        for x in self.concept.retrieve_one(cui)['rel']['child']:
            cuis.extend(self.get_all_children(x))
        return cuis
