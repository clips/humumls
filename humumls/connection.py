"""Connection wrapper around mongoclient."""
from pymongo import MongoClient


class Connection(object):
    """
    Connection class, used to pass collections to multiple Table objects.

    All params are equivalent to the pymongo params.

    For a standard installation, use default params, e.g.
        dbname = "umls"
        hostname = "localhost"
        port = 27107

    Parameters
    ----------
    dbname : string
        The name of the database.
    hostname : string
        The hostname.
    port : int
        The port to connect to.

    Attributes
    ----------
    client : MongoClient
        The initialized mongoclient.
    db : MongoDB.DB
        The specific database queried by this connection.

    """

    def __init__(self, dbname="umls", hostname="localhost", port=27017):
        """Create a new connection to a specified database."""
        self.client = MongoClient(host=hostname, port=port)
        self.db = self.client.get_database(dbname)
