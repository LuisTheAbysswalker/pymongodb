#!/usr/bin/python
# -*- coding: utf-8 -*-
from typing import Dict, List, Set, Union
from six import add_metaclass
from django.conf import settings
from typing import Type, TypedDict, Optional
from pymongo.errors import DuplicateKeyError, BulkWriteError
from pymongo import InsertOne, DeleteOne, UpdateOne
from pymongo.cursor import _Hint
from pymongo.results import UpdateResult, DeleteResult
from pymongodb.exceptions import DoesNotExist, BulkWriteException, AttributesEmptyException
from pymongodb.connection import Connection
from pymongodb.attributes import Attribute, AttributeContainer, ObjectIdAttribute, AttributeContainerMeta
from pymongodb.constants import META_CLASS_NAME, BATCH_WRITE_PAGE_LIMIT, ADD, DELETE, UPDATE, INC


class DefaultMeta(object):
    pass


class MetaModel(AttributeContainerMeta):
    """
    Model metaclass

    """
    def __init__(self, name, bases, attrs):
        super(MetaModel, self).__init__(name, bases, attrs)
        for attr_name, attribute in self.get_attributes().items():
            if attribute.is_hash_key:
                self._hash_key = attr_name

        if isinstance(attrs, dict):
            for attr_name, attr_obj in attrs.items():
                if isinstance(attr_obj, Attribute) and attr_obj.attr_name is None:
                    attr_obj.attr_name = attr_name

            if META_CLASS_NAME not in attrs:
                setattr(self, META_CLASS_NAME, DefaultMeta)

            # create a custom Model.DoesNotExist derived from pymongodb.exceptions.DoesNotExist,
            # so that "except Model.DoesNotExist:" would not catch other models' exceptions
            if 'DoesNotExist' not in attrs:
                exception_attrs = {'__module__': attrs.get('__module__')}
                if hasattr(self, '__qualname__'):  # On Python 3, Model.DoesNotExist
                    exception_attrs['__qualname__'] = '{}.{}'.format(self.__qualname__, 'DoesNotExist')
                self.DoesNotExist = type('DoesNotExist', (DoesNotExist,), exception_attrs)


@add_metaclass(MetaModel)
class MongoDBModel(AttributeContainer):

    class Meta:
        db_name = ""
        table_name = ""

    _connection = None
    _collection = None
    _id = ObjectIdAttribute()
    _hash_key = None
    DoesNotExist = DoesNotExist
    DuplicateKeyExist = DuplicateKeyError
    BulkWriteException = BulkWriteException
    AttrsEmptyException = AttributesEmptyException

    def __init__(self, hash_key=None, **attributes):
        """
        :param hash_key: Required. The hash key for this object.
        :param range_key: Only required if the table has a range key attribute.
        :param attrs: A dictionary of attributes to set on this object.
        """
        if hash_key is not None:
            attributes[self._hash_key] = hash_key
        super(MongoDBModel, self).__init__(**attributes)

    @property
    def mongo_obj_id(self):
        return self._id

    @classmethod
    def _get_connection(cls):
        if cls._connection is None or cls._collection is None:
            cls._connection = Connection.get_connection()

            db_name = cls.Meta.db_name or settings.MONGODB_DEFAULT_DATABASE
            db = cls._connection[db_name]
            cls._collection = db[cls.Meta.table_name]
        return cls._connection, cls._collection

    def _deserialize(self, attrs):
        """
        Sets attributes sent back from DynamoDB on this object

        :param attrs: A dictionary of attributes to update this item with.
        """
        for name, attr in self.get_attributes().items():
            value = attrs.get(attr.attr_name, None)
            if value is not None:
                value = attr.deserialize(value)
            setattr(self, name, value)

    @classmethod
    def from_raw_data(cls, data: Dict):
        """
        Returns an instance of this class
        from the raw data

        :param data: A serialized MongoDB object
        """
        if data is None:
            raise ValueError("Received no data to construct object")

        attributes = {}
        model_attributes = cls.get_attributes()
        for attr_name, value in data.items():
            attr = model_attributes.get(attr_name, None)
            if attr:
                attributes[attr_name] = attr.deserialize(value)
        return cls(**attributes)

    def copy_from(self, model_from):
        for key, attribute in model_from.get_attributes().items():
            if key == '_id':
                continue
            if attribute.is_hash_key:
                continue
            if hasattr(self, key):
                setattr(self, key, getattr(model_from, key, None))
        return

    @classmethod
    def run(cls, async_task):
        connection, collection = cls._get_connection()
        loop = connection.get_io_loop()
        result = loop.run_until_complete(async_task)
        return result

    @classmethod
    def get(cls, attributes_to_get=None, **kwargs):
        _, collection = cls._get_connection()
        document = cls.run(collection.find_one(kwargs, attributes_to_get))

        if document:
            return cls.from_raw_data(document)
        raise cls.DoesNotExist()

    @classmethod
    async def async_get(cls, attributes_to_get=None, **kwargs):
        connection, collection = cls._get_connection()
        document = await collection.find_one(kwargs, attributes_to_get)
        if document:
            return cls.from_raw_data(document)
        raise cls.DoesNotExist()

    @classmethod
    async def async_find(cls,
                         condition: Dict,
                         sort: Optional[List] = None,
                         offset: int = 0,
                         limit: Optional[int] = None,
                         attributes_to_get: Optional[List] = None,
                         hint: Optional[_Hint] = None,
                         explain: bool = False):
        connection, collection = cls._get_connection()

        if attributes_to_get:
            fields_to_get = dict()
            for attr in attributes_to_get:
                fields_to_get[attr] = 1
        else:
            fields_to_get = None

        cursor = collection.find(condition, fields_to_get)
        if sort:
            cursor.sort(sort)
        if offset:
            cursor.skip(offset)
        if limit:
            cursor.limit(limit)

        if hint:
            cursor.hint(hint)

        documents = list()
        async for document in cursor:
            documents.append(cls.from_raw_data(document))

        if explain:
            print(await cursor.explain())
        return documents

    @classmethod
    def find(cls,
             condition: Dict,
             sort: Optional[List] = None,
             offset: int = 0,
             limit: Optional[int] = None,
             attributes_to_get: Optional[List] = None,
             hint: Optional[_Hint] = None,
             explain: bool = False):
        # mongo api
        return cls.run(cls.async_find(condition, sort, offset, limit, attributes_to_get, hint, explain))

    @classmethod
    async def async_query(cls,
                          sort: Optional[List] = None,
                          attributes_to_get: Optional[List] = None,
                          **kwargs):
        # ORM Invoke:
        # Model.query(user='xxxx')
        if "offset" in kwargs:
            offset = kwargs.pop("offset")
        else:
            offset = 0

        if "limit" in kwargs:
            limit = kwargs.pop("limit")
        else:
            limit = None
        return await cls.async_find(kwargs, sort, offset, limit, attributes_to_get)

    @classmethod
    def query(cls, attributes_to_get=None, **kwargs):
        return cls.run(cls.async_query(attributes_to_get, **kwargs))

    @classmethod
    def batch_get(cls, items: Union[List, Set], attributes_to_get=None):
        if items:
            return cls.run(cls.async_find({cls._hash_key: {'$in': list(items)}}, attributes_to_get=attributes_to_get))
        else:
            return list()

    @classmethod
    async def async_batch_get(cls, items: Union[List, Set], attributes_to_get=None):
        if items:
            return await cls.async_find({cls._hash_key: {'$in': list(items)}}, attributes_to_get=attributes_to_get)
        else:
            return list()

    def save(self):
        _, collection = self._get_connection()
        res = self.run(collection.insert_one(self.attribute_values))
        return res

    async def async_save(self):
        _, collection = self._get_connection()
        return await collection.insert_one(self.attribute_values)

    def delete(self) -> DeleteResult:
        _, collection = self._get_connection()
        if self._id:
            res = self.run(collection.delete_one({"_id": self._id}))
        elif self.attributes:
            res = self.run(collection.delete_one(self.attributes))
        else:
            raise self.AttrsEmptyException(f"delete error: {self.AttrsEmptyException.msg}")
        return res

    async def async_delete(self) -> DeleteResult:
        _, collection = self._get_connection()
        if self._id:
            res = await collection.delete_one({"_id": self._id})
        elif self.attributes:
            res = await collection.delete_one(self.attributes)
        else:
            raise self.AttrsEmptyException(f"delete error: {self.AttrsEmptyException.msg}")

        return res

    @classmethod
    async def async_delete_many(cls, **kwargs) -> DeleteResult:
        _, collection = cls._get_connection()
        res = await collection.delete_many(kwargs)
        return res

    def update(self, actions, reload=True, upsert=False) -> UpdateResult:
        _, collection = self._get_connection()
        if self._id:
            res = self.run(collection.update_one({"_id": self._id}, {'$set': actions}, upsert=upsert))
        elif self.attributes:
            res = self.run(collection.update_one(self.attributes, {'$set': actions}, upsert=upsert))
        else:
            raise self.AttrsEmptyException(f"update error: {self.AttrsEmptyException.msg}")

        if res.modified_count and reload:
            if self._id:
                new_data = self.get(_id=self._id)
            else:
                new_data = self.get(**self.attributes)
            self._deserialize(new_data.attribute_values)
        return res

    async def async_update(self, actions, reload=True, upsert=False) -> UpdateResult:
        """

        :param actions:
        :param reload: If ``True``, reload data from mongodb.
        :param upsert: If ``True``, perform an insert if no documents match the filter.
        :return:
        """
        _, collection = self._get_connection()
        if self._id:
            res = await collection.update_one({"_id": self._id}, {'$set': actions}, upsert=upsert)
        elif self.attributes:
            res = await collection.update_one(self.attributes, {'$set': actions}, upsert=upsert)
        else:
            raise self.AttrsEmptyException(f"update error: {self.AttrsEmptyException.msg}")
        if res.modified_count and reload:
            if self._id:
                new_data = await self.async_get(_id=self._id)
            else:
                new_data = await self.async_get(**self.attributes)
            self._deserialize(new_data.attribute_values)
        return res

    @classmethod
    async def async_update_many(cls, query_filter, actions, upsert=False) -> UpdateResult:
        """

        :param query_filter:
        :param actions:
        :param upsert: If ``True``, perform an insert if no documents match the filter.
        :return:
        """
        _, collection = cls._get_connection()
        res = await collection.update_many(query_filter, actions, upsert=upsert)
        return res

    def inc(self, actions) -> UpdateResult:
        _, collection = self._get_connection()
        if self._id:
            res = self.run(collection.update_one({"_id": self._id}, {'$inc': actions}))
        elif self.attributes:
            res = self.run(collection.update_one(self.attributes, {'$inc': actions}))
        else:
            raise self.AttrsEmptyException(f"inc error: {self.AttrsEmptyException.msg}")
        return res

    async def async_inc(self, actions) -> UpdateResult:
        _, collection = self._get_connection()
        if self._id:
            res = await collection.update_one({"_id": self._id}, {'$inc': actions})
        elif self.attributes:
            res = await collection.update_one(self.attributes, {'$inc': actions})
        else:
            raise self.AttrsEmptyException(f"inc error: {self.AttrsEmptyException.msg}")
        return res

    @classmethod
    def count(cls, filters) -> int:
        _, collection = cls._get_connection()
        return cls.run(collection.count_documents(filters))

    @classmethod
    async def async_count(cls, filters) -> int:
        _, collection = cls._get_connection()
        return await collection.count_documents(filters)

    @classmethod
    async def bulk_write(cls, requests: List, ordered: bool = False):
        """
        :param requests: A list of write operations (see examples above).
        :param ordered: If ``True`` requests will be performed on the server serially,
            in the order provided. If an error occurs all remaining operations are aborted.
            If ``False`` requests will be performed on the server in arbitrary order, possibly in
            parallel, and all operations will be attempted.
        :return:
        """
        _, collection = cls._get_connection()
        return await collection.bulk_write(requests, ordered=ordered)

    @classmethod
    def batch_write(cls):
        """
        Returns a BatchWrite context manager for a batch operation.
        """
        return BatchWrite(cls)

    @classmethod
    async def create_index(cls,
                           index: Union[str, List],
                           name: Optional[str] = "",
                           unique: bool = False,
                           expire_after_seconds: Optional[int] = None,
                           partial_filter_expression: Optional[Dict] = None):
        """
        create mongodb index
        :param index: To create a single key ascending index on the key 'user_id' we just use a string argument:
                create_index("user_id")
                For a compound index on 'mike' descending and 'eliot' ascending we need to use a list of tuples:
                await collection.create_index([("user_id", pymongo.DESCENDING),
                                                  ("time", pymongo.ASCENDING)])
        :param name: custom name to use for this index - if none is given, a name will be generated.
        :param unique: if True creates a uniqueness constraint on the index.
        :param expire_after_seconds: Optional. Specifies a value, in seconds,
                as a time to live (TTL) to control how long MongoDB retains documents in this collection.
                This option only applies to TTL indexes.
        :param partial_filter_expression: A document that specifies a filter for a partial index.
        :return:
        """
        _, collection = cls._get_connection()
        index_settings = dict()
        if name:
            index_settings["name"] = name
        if unique:
            index_settings["unique"] = unique
        if expire_after_seconds is not None:
            index_settings["expireAfterSeconds"] = expire_after_seconds
        if partial_filter_expression is not None:
            index_settings["partialFilterExpression"] = partial_filter_expression
        await collection.create_index(index, **index_settings)

    @classmethod
    async def aggregate(cls, pipeline, *args, **kwargs):
        """
        :param pipeline: A list of aggregation pipeline stages.
        :return:
        """
        _, collection = cls._get_connection()
        documents = list()
        async for document in collection.aggregate(pipeline, *args, **kwargs):
            documents.append(document)
        return documents


class BatchWriteOperationDict(TypedDict, total=False):
    action: int
    item: Type[MongoDBModel]
    update: Optional[Dict]
    upsert: bool


class BatchWrite(object):
    def __init__(self, mongo_model: Type[MongoDBModel], ordered: bool = False):
        self.model = mongo_model
        self.pending_operations: List[BatchWriteOperationDict] = []
        self.failed_operations: List[BatchWriteOperationDict] = []
        self.max_operations = BATCH_WRITE_PAGE_LIMIT
        self.ordered = ordered

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        This ensures that all pending operations are committed when
        the context is exited
        """
        return await self.commit()

    """
    A class for batch writes
    """
    async def save(self, put_item: Type[MongoDBModel]):
        """
        This adds `put_item` to the list of pending operations to be performed.

        :param put_item: Should be an instance of a `Model` to be written
        """
        if len(self.pending_operations) == self.max_operations:
            await self.commit()
        self.pending_operations.append({"action": ADD, "item": put_item})

    async def delete(self, del_item: Type[MongoDBModel]):
        """
        This adds `del_item` to the list of pending operations to be performed.

        :param del_item: Should be an instance of a `Model` to be deleted
        """
        if len(self.pending_operations) == self.max_operations:
            await self.commit()
        self.pending_operations.append({"action": DELETE, "item": del_item})

    async def update(self, update_item: Type[MongoDBModel], update_data: Dict, upsert: bool = False):
        """
        This adds `update_item` to the list of pending operations to be performed.

        :param update_item: Should be an instance of a `Model` to be updated
        :param update_data: The modifications to apply.
        :param upsert: If ``True``, perform an insert if no documents match the filter.
        """
        if len(self.pending_operations) == self.max_operations:
            await self.commit()
        self.pending_operations.append({"action": UPDATE, "item": update_item, "update": update_data, "upsert": upsert})

    async def inc(self, inc_item: Type[MongoDBModel], inc_data: Dict, upsert: bool = False):
        """
        This adds `update_item` to the list of pending operations to be performed.

        :param inc_item: Should be an instance of a `Model` to be updated
        :param inc_data: The modifications to apply.
        :param upsert: If ``True``, perform an insert if no documents match the filter.
        """
        if len(self.pending_operations) == self.max_operations:
            await self.commit()
        self.pending_operations.append({"action": INC, "item": inc_item, "update": inc_data, "upsert": upsert})

    async def commit(self):
        """
        Writes all the changes that are pending
        """
        mongo_requests = []
        for item in self.pending_operations:
            mongo_model = item['item']
            if item['action'] == ADD:
                mongo_requests.append(InsertOne(mongo_model.attribute_values))
            elif item['action'] == DELETE:
                if mongo_model.mongo_obj_id:
                    mongo_requests.append(DeleteOne({'_id': mongo_model.mongo_obj_id}))
                else:
                    mongo_requests.append(DeleteOne(mongo_model.attributes))
            elif item['action'] == UPDATE:
                upsert = item['upsert']
                if mongo_model.mongo_obj_id:
                    mongo_requests.append(UpdateOne({'_id': mongo_model.mongo_obj_id}, {'$set': item['update']}, upsert=upsert))
                else:
                    mongo_requests.append(UpdateOne(mongo_model.attributes, {'$set': item['update']}, upsert=upsert))
            elif item['action'] == INC:
                upsert = item['upsert']
                if mongo_model.mongo_obj_id:
                    mongo_requests.append(UpdateOne({'_id': mongo_model.mongo_obj_id}, {'$inc': item['update']}, upsert=upsert))
                else:
                    mongo_requests.append(UpdateOne(mongo_model.attributes, {'$inc': item['update']}, upsert=upsert))

        self.pending_operations = []
        if not mongo_requests:
            return

        try:
            ret = await self.model.bulk_write(mongo_requests, self.ordered)
        except BulkWriteError as err:
            raise self.model.BulkWriteException(err.details)
