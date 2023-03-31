#!/usr/bin/python
# -*- coding: utf-8 -*-


class MongoDBException(Exception):
    """
    A common exception class
    """
    def __init__(self, msg=None, cause=None):
        self.msg = msg or self.msg
        self.cause = cause
        super(MongoDBException, self).__init__(self.msg)


class DoesNotExist(MongoDBException):
    """
    Raised when an item queried does not exist
    """
    msg = "Item does not exist"


class BulkWriteException(Exception):
    """
    Raised when bulk write error
    """
    pass


class AttributesEmptyException(Exception):
    """
    Raised when model's attributes are nothing
    """
    msg = "should get model from db or set model attributes first"
