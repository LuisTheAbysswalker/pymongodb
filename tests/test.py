#!/usr/bin/python
# -*- coding: utf-8 -*-
from pymongodb.models import MongoDBModel
from pymongodb.attributes import UnicodeAttribute, ListAttribute, NumberAttribute


class MovieModel(MongoDBModel):

    class Meta(MongoDBModel.Meta):
        db_name = "examples"
        table_name = "movies"

    movie_id = UnicodeAttribute(hash_key=True)
    title = UnicodeAttribute(default="CN")
    languages = ListAttribute()
    year = NumberAttribute()
    genres = ListAttribute()


class ScheduleCacheModel(MongoDBModel):

    class Meta(MongoDBModel.Meta):
        table_name = "schedule_cache"

    key = UnicodeAttribute(hash_key=True)
    value = JSONAttribute(null=True)
    time = NumberAttribute(default=0)
    ttl = UTCDateTimeAttribute(null=True)


if __name__ == "__main__":
    # save
    movie = MovieModel(movie_id='xxxxxxx')
    movie.title = 'asdasd'
    movie.year = 1988
    movie.languages = ["Chinese", "English"]
    movie.save()

    # delete
    print(movie.delete())

    # get(mongo api)
    movie = MovieModel.get(title='Casablanca')

    # update
    res = movie.update({'title': 'Casablanca', 'year': 1943})
    print(res)

    # query
    # cursor = MovieModel.query(title="Casablanca", year=1943)
    cursor = MovieModel.query(offset=0, limit=100, title="Casablanca", year=1943)
    for i in cursor:
        print(i)

    # query(mongo api)
    cursor = MovieModel.find({'languages': {'$in': ["Japanese", "Mandarin"]}})
    for i in cursor:
        print(i)

    # region batch write test
    from notification.mongo_models import ScheduleCacheModel
    async with ScheduleCacheModel.batch_write() as batch:
        scm = ScheduleCacheModel(key="d")
        await batch.save(scm)

        scm2 = ScheduleCacheModel(key="e")
        await batch.save(scm2)

        scm2 = ScheduleCacheModel(key="f")
        await batch.save(scm2)

        await batch.delete(ScheduleCacheModel(key="Push_46a43585aea047febe6593f93031205c"))
        await batch.update(ScheduleCacheModel(key="a"), {"value": {"a": 1}})

        c = await ScheduleCacheModel.async_get(key="c")
        await batch.update(c, {"value": {"c": 1}})

        b = await ScheduleCacheModel.async_get(key="b")
        await batch.delete(b)
    # endregion

    # aggregate
    movie_ids = [movie_agg['_id'] for movie_agg in await MovieModel.aggregate([
        {
            "$match": {
                'title': "xxx",
                'year': {'$gt': 2020},
                'movie_id': {'$ne': "movie_id"}
            }
        },
        {
            "$group": {"_id": "movie_id"}
        }
    ])]
