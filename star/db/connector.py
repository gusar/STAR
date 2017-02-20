from star.db.mongo import Mongo


DB_OBJECT_MAP = {
    'mongo': Mongo
}


class DBConnector(object):
    """
    Ubiquitous database connection object creator
    """
    def __new__(cls, db_type, **kwargs):
        """
        :param db_type: str: key from DB_OBJECT_MAP
        :param kwargs: arguments required to initialize the required db connection
        :return: DB connection object of respective type
        """
        return DB_OBJECT_MAP[db_type](**kwargs)

