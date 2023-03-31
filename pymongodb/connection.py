#!/usr/bin/python
# -*- coding: utf-8 -*-
import asyncio
import motor.motor_asyncio
from django.conf import settings


class Connection(object):

    _connection = None

    @classmethod
    def get_or_create_eventloop(cls):
        try:
            return asyncio.get_event_loop()
        except RuntimeError as ex:
            if "There is no current event loop in thread" in str(ex):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                return asyncio.get_event_loop()
            raise Exception(ex)

    @classmethod
    def get_connection(cls):
        if cls._connection is None:
            event_loop = cls.get_or_create_eventloop()
            cls._connection = motor.motor_asyncio.AsyncIOMotorClient(
                settings.MONGODB_CONNECTION_URI,
                io_loop=event_loop,
                serverSelectionTimeoutMS=5000
            )
        return cls._connection
