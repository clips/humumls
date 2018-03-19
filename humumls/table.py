"""Table classes, both specific for UMLS and base classes."""


class Table(object):
    """
    Base class for all Table classes.

    Parameters
    ----------
    connection : Connection
        A humumls.connection.Connection instance which is used to connect
        to the mongoDB.
    classname : string
        The name of the collection which is used by this table.

    """

    def __init__(self, connection, classname):
        """Init method."""
        self.classname = classname
        self.connection = connection
        self._connection = self.connection.db.get_collection(self.classname)

    def __getitem__(self, key):
        """
        Syntactic sugar.

        Parameters
        ----------
        key : object
            An instance of a primary key for the given collection this
            database queries. For UMLS, these are invariably strings.

        Returns
        -------
        record : dict
            A single record.

        """
        return self.retrieve_one({"_id": key})

    def retrieve(self, query, filt=()):
        """
        Retrieve items from the collection.

        Parameters
        ----------
        query : dict
            The mongoDB query to run.
        filt : dict
            The filter dictionary to use.

        Returns
        -------
        cursor : MongoDB Cursor.
            A cursor pointing towards the objects.

        """
        if filt:
            return self._connection.find(query, filt)
        else:
            return self._connection.find(query)

    def retrieve_one(self, query, filt=()):
        """
        Retrieve a single item from the collection.

        If multiple items satisfy the query, only the first one is returned.

        Parameters
        ----------
        query : dict
            The mongoDB query to run.
        filt : dict
            The filter dictionary to use.

        Returns
        -------
        cursor : MongoDB Cursor.
            A cursor pointing towards the objects.

        """
        if filt:
            return self._connection.find_one(query, filt)
        else:
            return self._connection.find_one(query)

    def bunch(self, ids, filt=(), orq=True):
        """
        Return a bunch of items based on their primary keys.

        Parameters
        ----------
        ids : list of objects
            A list of IDs to retrieve.
        filt : dict
            The filtering query.
        orq : bool
            Whether to use an OR or an IN query

        """
        if not ids:
            return []
        if orq:
            return self.retrieve({"$or": [{"_id": i}
                                  for i in ids]}, filt)
        else:
            return self.retrieve({"_id": {"$in": ids}}, filt)


class String(Table):
    """Connection to the String collection."""

    def __init__(self, connection):
        """Init method."""
        super(String, self).__init__(connection, "string")

    def surface(self, ids, lower=True):
        """
        Retrieve the surface form of a list of string ids.

        Parameters
        ----------
        ids : list of string
            A list of ids (SUI in UMLS terminology)
        lower : bool
            Whether to return the lower-cased version or the original version.

        Returns
        -------
        forms : list of string
            A list of strings for the given ids.

        """
        if lower:
            return [s["lower"] for s in self.bunch(ids)]
        else:
            return [s["string"] for s in self.bunch(ids)]

    def cui(self, surface):
        """
        Retrieve all cuis associated with a given surface form.

        Parameters
        ----------
        surface : string
            The string for which to retrieve the cuis.

        Returns
        -------
        cuis : list of string
            A list of cuiS which share the surface form.

        """
        # Filter: don't retrieve the _id itself.
        string = self.retrieve_one({"string": surface},
                                   {"_id": 0, "cui": 1})
        if string:
            return string["cui"]
        else:
            return []


class Concept(Table):
    """Connection to the Concept collection."""

    def __init__(self, connection):
        """Init method."""
        super(Concept, self).__init__(connection, "concept")

    def all_definitions(self):
        """
        Get all concepts with definitions.

        Returns
        -------
        concepts : dict
            A dictionary where the key is the Concept ID and the value is a
            list of definitions for said concept.

        """
        return {x["_id"]: x["description"]
                for x in self.retrieve({"$exists": "definition"},
                                       {"definition": 1})}

    def bunch_definitions(self, cuis):
        """
        Get definitions for a bunch of concept ids.

        Parameters
        ----------
        cuis : list of strings
            A list of concept ids (cui)

        Returns
        -------
        concepts : dict
            A dictionary with the as the concept ID and the value is a list of
            definitions.

        """
        return {c["_id"]: c["definition"]
                for c in self.bunch(cuis, {"definition": 1}, orq=True)}

    def one_definition(self, cui):
        """
        Get all definitions for a single concept.

        Parameters
        ----------
        cui : a single concept ID

        Returns
        -------
        descriptions : list
            A list of descriptions.

        """
        return self[cui]["description"]

    def preferred(self, cui):
        """Get the preferred term associated with a single concept id."""
        return self[cui]["preferred"]

    def synonym(self, cui):
        """Get the cuis of the concepts which are synonyms of the given cui."""
        return self[cui]["rel"]["synonym"]

    def words(self, cui):
        """Get all words associated with a concept ID."""
        return self[cui]["string"]

    def children(self, cui):
        """Get all cuis of concepts which are children of this concept."""
        return self[cui]["rel"]["child"]


class Term(Table):
    """Connection to the Term collection."""

    def __init__(self, connection):
        """Init method."""
        super(Term, self).__init__(connection, "term")
