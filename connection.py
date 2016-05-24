from pymongo import MongoClient


class Connection:
    """Connection class, used to pass through databases to multiple Table objects"""

    def __init__(self, dbname="umls", hostname="localhost", port=27017):
        """
        Creates a new connection to a specified database.

        All params are equivalent to the pymongo params.

        For a standard installation, use default params, e.g.
            dbname = "umls"
            hostname = "localhost"
            port = 27107

        :param dbname: The name of the database.
        :param hostname: The hostname.
        :param port: The port.
        :return: None
        """
        self.client = MongoClient(host=hostname, port=port)
        self.db = self.client.get_database(dbname)

