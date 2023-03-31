#!/usr/bin/python
# -*- coding: utf-8 -*-
import six
from six import add_metaclass
from inspect import getmembers


class Attribute(object):

    null = False

    def __init__(self, hash_key=False, null=None, default=None, attr_name=None):
        self.default = default
        self.is_hash_key = hash_key
        self.attr_path = [attr_name]
        if hash_key:
            self.null = False
        elif null is not None:
            self.null = null

    @property
    def attr_name(self):
        return self.attr_path[-1]

    @attr_name.setter
    def attr_name(self, value):
        self.attr_path[-1] = value

    def serialize(self, value):
        """
        This method should return a dynamodb compatible value
        """
        return value

    def deserialize(self, value):
        """
        Performs any needed deserialization on the value
        """
        return value

    def __set__(self, instance, value):
        if instance:
            instance.attribute_values[self.attr_name] = value

    def __get__(self, instance, instance_type):
        if instance:
            return instance.attribute_values.get(self.attr_name, None)
        else:
            return self


class NumberAttribute(Attribute):
    """
    A number attribute
    """


class BooleanAttribute(Attribute):
    """
    A class for boolean attributes
    """

    def serialize(self, value):
        if value is None:
            return None
        elif value:
            return True
        else:
            return False

    def deserialize(self, value):
        return bool(value)


class UnicodeAttribute(Attribute):
    """A unicode string attribute."""

    def serialize(self, value):
        """
        Returns a unicode string
        """
        if value is None or not len(value):
            return None
        elif isinstance(value, six.text_type):
            return value
        else:
            return six.u(value)


class UTCDateTimeAttribute(Attribute):
    """
    An attribute for storing a UTC Datetime
    """


class JSONAttribute(Attribute):
    """
    A dictionary field that wraps a standard Python dictionary.
    """

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("default", lambda: {})
        super().__init__(*args, **kwargs)


class ObjectIdAttribute(Attribute):
    """A ObjectId attribute."""


class ListAttribute(Attribute):
    """
    A list attribute that wraps a standard field, allowing multiple instances
    of the field to be used as a list in the database.
    """

    def __init__(self, **kwargs):
        kwargs.setdefault("default", lambda: [])
        super().__init__(**kwargs)


class AttributeContainerMeta(type):

    def __init__(cls, name, bases, attrs):
        super(AttributeContainerMeta, cls).__init__(name, bases, attrs)
        AttributeContainerMeta._initialize_attributes(cls)

    @staticmethod
    def _initialize_attributes(cls):
        """
        Initialize attributes on the class.
        """
        cls._attributes = {}

        for name, attribute in getmembers(cls, lambda o: isinstance(o, Attribute)):
            cls._attributes[name] = attribute
            attribute.attr_name = name


@add_metaclass(AttributeContainerMeta)
class AttributeContainer(object):

    def __init__(self, **attributes):
        # The `attribute_values` dictionary is used by the Field data descriptors in cls._fields
        # to store the values that are bound to this instance. Fields store values in the dictionary
        # using the `python_attr_name` as the dictionary key. "Raw" (i.e. non-subclassed) MapAttribute
        # instances do not have any Attributes defined and instead use this dictionary to store their
        # collection of name-value pairs.
        self.attributes = attributes
        # all values contain default value
        self.attribute_values = {}
        self._set_defaults()
        self._set_attributes(**attributes)

    def _set_defaults(self):
        """
        Sets and fields that provide a default value
        """
        for name, attr in self.get_attributes().items():
            default = attr.default
            if callable(default):
                value = default()
            else:
                value = default
            if value is not None:
                setattr(self, name, value)

    @classmethod
    def get_attributes(cls):
        """
        Returns the attributes of this class as a mapping from `python_attr_name` => `attribute`.

        :rtype: dict[str, Attribute]
        """
        return cls._attributes

    def _set_attributes(self, **attributes):
        """
        Sets the attributes for this object
        """
        for attr_name, attr_value in six.iteritems(attributes):
            if attr_name not in self.get_attributes():
                raise ValueError("Attribute {} specified does not exist".format(attr_name))
            setattr(self, attr_name, attr_value)
