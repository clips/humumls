
class Table(object):
    """Base class for all Table classes."""

    def __init__(self, connection, classname):
        """

        :param connection: a connection instance.
        :param classname: the name of the class for which the crown functions.
        :return:
        """
        self.classname = classname
        self.connection = connection
        self._connection = self.connection.db.get_collection(self.classname)

    def __getitem__(self, key):
        """
        Syntactic sugar.

        :param key: The _id of the current class.
        :return: A single record or an empty list.
        """
        return self._connection.find_one({"_id": key})

    def retrieve(self, query, filt=()):
        """
        Retrieves items from the connection.

        :param query: The query to run.
        :param filt: The filter.
        :return: A cursor with the filter.
        """
        if filt:
            return self._connection.find(query, filt)
        else:
            return self._connection.find(query)

    def retrieve_one(self, query, filt=()):
        """
        Retrieve a single item from the connection.

        :param query: The query to run.
        :param filt: The filter.
        :return: A single item.
        """
        if filt:
            return self._connection.find_one(query, filt)
        else:
            return self._connection.find_one(query)

    def bunch(self, listofids, filt=(), orq=True):
        """
        Return a bunch of items based on primary keys.

        :param listofids: a list of IDs to retrieve.
        :param orq: whether to use an OR or an IN query.
        :return: a cursor to the specified items.
        """
        if orq:
            return self.retrieve({"$or": [{"_id": i} for i in listofids]}, filt)
        else:
            return self.retrieve({"_id": {"$in": listofids}}, filt)


class String(Table):
    """Connection to the String collection"""

    def __init__(self, connection):
        """


        :param connection: an instance of a Connection class.
        :return:
        """

        super(String, self).__init__(connection, "string")

    def surface(self, listofids, lower=True):
        """
        Retrieve the surface form of a list of string ids.

        :param listofids: A list of ids (SUI in UMLS terminology)
        :param lower: whether to return the lower-cased version or the original version.
        :return: a list of surface forms.
        """

        if lower:
            return [s["lower"] for s in self.bunch(listofids)]
        else:
            return [s["string"] for s in self.bunch(listofids)]

    def concept_id(self, surface):
        """
        Retrieves all concept ids associated with a given surface form.

        :param surface: The surface form for which to retrieve all concept ids.
        :return:
        """

        string = self.retrieve_one({"string": surface}, {"_id": 0, "concept": 1})
        if string:
            return string["concept"]
        else:
            return []


class Concept(Table):
    """Connection to the Concept collection"""

    def __init__(self, connection):

        super(Concept, self).__init__(connection, "concept")

    def all_definitions(self):
        """
        Returns all definitions

        :return: A dictionary where the key is the Concept ID and the value is a list of definitions.
        """
        return {x["_id"]: x["description"] for x in self.retrieve({"$exists": "definition"}, {"definition": 1})}

    def bunch_definitions(self, cids):
        """
        Returns the definitions for a bunch of concept ids.

        :param cids: A list of concept ids (CUI)
        :return: A dictionary where the key is the concept ID and the value is a list of definitions.
        """
        return {c["_id"]: c["definition"] for c in self.bunch(cids, {"definition": 1}, orq=True)}

    def one_definition(self, cid):
        """
        Return all definitions for a single concept.

        :param cid: A single cid.
        :return: A list of descriptions.
        """
        return self[cid]["description"]

    def get_preferred(self, cid):
        """
        Gets the preferred term associated with a single concept id.

        :param cid: a concept id.
        :return: the TID of the preferred term.
        """
        return self[cid]["preferred"]

    def get_synonym(self, cid):
        """
        Gets the cids of the concepts which are synonyms of the given cid.

        :param cid: the cid.
        :return: A list of concept that are synonyms to the given cid.
        """

        return self[cid]["rel"]["synonym"]

    def get_words(self, cid):
        """
        Gets all words which are associated with a concept ID.

        :param cid: The concept ID
        :return: A list of words.
        """

        return self[cid]["string"]


class Term(Table):
    """Connection to the Term collection"""

    def __init__(self, connection):

        super(Term, self).__init__(connection, "term")