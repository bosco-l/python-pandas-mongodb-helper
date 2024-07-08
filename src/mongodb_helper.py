import pandas as pd

from pymongo import UpdateOne
from pymongo.collection import Collection
from pymongo.mongo_client import MongoClient
from pymongo.database import Database
from typing import List, Any, Optional


class MongoDBHelper:
    def __init__(self, uri: str = None):
        """
        Initialize MongoDBHelper instance.

        Parameters
        ----------
        uri : str, optional
            The URI for the MongoDB instance. Default is None.
        """
        self.mongo_client = None
        self.uri = uri
        # self.uri = "mongodb://localhost:27017/"  # debug/testing
        self._database = None
        self._collection = None

        self._connect_to_mongodb()

    def _connect_to_mongodb(self) -> None:
        """
        Connect to a MongoDB instance.
        """
        if self.uri is None:
            raise ValueError('Please specify MongoDB instance URI')

        try:
            self.mongo_client = MongoClient(self.uri)
        except Exception as e:
            raise ConnectionError(f'Error connecting to MongoDB: {e}')

    @property
    def database(self) -> Database:
        return self._database

    @database.setter
    def database(self, database_name: str) -> None:
        """
        Set a :class:`pymongo.database.Database` object as the value of the database property.

        A new database will be created if it does not exist.

        Parameters
        ----------
        database_name : str
            The name of the MongoDB database.

        Returns
        -------
        None
        """
        if self._database is None or (database_name != self._database.name):
            self._database = self.mongo_client[database_name]

    @property
    def collection(self) -> Collection:
        return self._collection

    @collection.setter
    def collection(self, collection_name: str) -> None:
        """
        Set a :class:`pymongo.collection.Collection` object as the value of the collection property.

        A new collection will be created if it does not exist.

        Parameters
        ----------
        collection_name : str
            The name of the MongoDB collection.

        Returns
        -------
        None
        """
        if self._collection is None or (self._collection.name != collection_name):
            self._collection = self.database[collection_name]

    def get_database_list(self) -> List[str]:
        """
        Retrieve existing database list.

        Returns
        -------
        list of str
            A list of database names.
        """
        return self.mongo_client.list_database_names()

    def delete_database(self, database_name: str) -> None:
        """
        Delete a MongoDB database.

        Parameters
        ----------
        database_name : str
            The name of the MongoDB database.

        Returns
        -------
        None
        """
        if database_name in self.get_database_list():
            self.mongo_client.drop_database(database_name)

            # Verify deletion
            if database_name not in self.get_database_list():
                print(f"Deleted database: {database_name}")
            else:
                print(f"Failed to delete database: {database_name}")
        else:
            print(f"This database doesn't exist: {database_name}")

    def get_collection_list(self, database_name: str) -> List[str]:
        """
        Retrieve a list of all collection names in a database.

        Parameters
        ----------
        database_name : str
            The name of the MongoDB database.

        Returns
        -------
        list of str
            A list of collection names in the database.
        """
        self.database = database_name
        return self.database.list_collection_names()

    def delete_collection(self, database_name: str, collection_name: str) -> None:
        """
        Delete a collection.

        Parameters
        ----------
        database_name : str
            The name of the MongoDB database.
        collection_name : str
            The name of the MongoDB collection.

        Returns
        -------
        None
        """
        self.database = database_name
        self.collection = collection_name
        r = self.database.drop_collection(collection_name)
        if r['ok'] == 1:
            print(f'Deleted collection: {database_name}.{collection_name}')
        else:
            print(f'Cannot delete collection: {database_name}.{collection_name}')

        # Alternative
        # self.collection.drop()

    def clean_collection(self, database_name: str, collection_name: str) -> None:
        """
        Delete all documents within a collection.

        Parameters
        ----------
        database_name : str
            The name of the MongoDB database.
        collection_name : str
            The name of the MongoDB collection.

        Returns
        -------
        None
        """
        self.database = database_name
        self.collection = collection_name
        r = self.collection.delete_many({})
        if r.acknowledged:
            print(f"Cleaned collection: {database_name}.{collection_name}")

    def insert_one_document(self, database_name: str, collection_name: str, document: dict) -> None:
        """
        Insert a single document into a collection of a database.

        Parameters
        ----------
        database_name : str
            The name of the MongoDB database.
        collection_name : str
            The name of the MongoDB collection.
        document : dict
            The document to be inserted.

        Returns
        -------
        None
        """
        self.database = database_name
        self.collection = collection_name
        r = self.collection.insert_one(document)
        if r.acknowledged:
            print(f"Inserted 1 document into: {database_name}.{collection_name}")

    def get_one_document(self,
                         database_name: str,
                         collection_name: str,
                         filter: dict = None,
                         *args: Any,
                         **kwargs: Any
                         ) -> Optional[dict]:
        """
        Get a single document from a collection.

        Parameters
        ----------
        database_name : str
            The name of the MongoDB database.
        collection_name : str
            The name of the MongoDB collection.
        filter : dict, optional
            A dictionary specifying the query.
        *args : Any
            Variable length argument list.
        **kwargs : Any
            Arbitrary keyword arguments.

        Returns
        -------
        dict or None
            A dictionary representing the document.
        """
        self.database = database_name
        self.collection = collection_name
        result = self.collection.find_one(filter, *args, **kwargs)
        return result

    def delete_one_document(self, database_name: str, collection_name: str, filter: dict, *args, **kwargs) -> None:
        """
        Delete a single document from a collection.

        Parameters
        ----------
        database_name : str
            The name of the MongoDB database.
        collection_name : str
            The name of the MongoDB collection.
        filter : dict
            A dictionary specifying the query that matches the document.

        Returns
        -------
        None
        """
        self.database = database_name
        self.collection = collection_name
        result = self.collection.delete_one(filter, *args, **kwargs)
        if result.acknowledged:
            print(f'Deleted {result.deleted_count} document in {database_name}.{collection_name}')

    def get_document_count(self, database_name: str, collection_name: str, filter: dict = None) -> int:
        """
        Parameters
        ----------
        database_name : str
            The name of the MongoDB database.
        collection_name : str
            The name of the MongoDB collection.
        filter : dict, optional
            A dictionary representing query string. Default is None.

        Returns
        -------
        int
            The count of documents that mater the filter.
        """
        self.database = database_name
        self.collection = collection_name
        count = self.collection.count_documents(filter)
        return count

    def upsert_df(self, df: pd.DataFrame, database_name: str, collection_name: str, primary_key_column: str):
        """
        Upsert a pandas DataFrame into a MongoDB collection.

        This method converts the DataFrame to a list of dictionaries and performs an upsert operation
        on each dictionary using the specified primary key column. If a document with the same primary key
        already exists in the collection, it will be updated. If no document with the primary key exists,
        a new document will be inserted.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to be upserted.
        database_name : str
            The name of the MongoDB database.
        collection_name : str
            The name of the MongoDB collection.
        primary_key_column : str
            The name of the column in the DataFrame that represents the primary key.

        Returns
        -------
        None
        """
        self.database = database_name
        self.collection = collection_name

        documents = df.to_dict('records')
        operations = [UpdateOne(filter={primary_key_column: doc[primary_key_column]},
                                update={'$set': doc},
                                upsert=True,
                                ) for doc in documents]
        result = self.collection.bulk_write(operations)
        if result.acknowledged:
            print(
                f"For {database_name}.{collection_name} - "
                f"{result.matched_count} matched, "
                f"{result.modified_count} modified, "
                f"{result.upserted_count} upserted"
            )

    def get_df(self, database_name: str, collection_name: str, filter: dict = None) -> pd.DataFrame:
        """
        Retrieves data from a MongoDB collection and returns it as a pandas DataFrame.

        Parameters
        ----------
        database_name : str
            The name of the MongoDB database.
        collection_name : str
            The name of the MongoDB collection.
        filter : dict, optional
            A dictionary representing the query string. Default is None.

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame containing the data from MongoDB
        """
        self.database = database_name
        self.collection = collection_name
        list_dict = list(self.collection.find(filter))
        df = pd.DataFrame(list_dict)
        return df
