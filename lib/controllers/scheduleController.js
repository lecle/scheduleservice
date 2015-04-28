"use strict";

exports.create = function(req, res, container) {

    var data = req.data;

    container.getService('MONGODB').then(function(service) {

        // check the PK
        service.send('findOne', {collectionName : 'schedule', query : {where : {name : data.name}}}, function(err, doc) {

            if(err)
                return res.error(err);

            if(doc.data) {

                return res.error(409, new Error("Duplicated unique property error"));
            }

            if(data.start && !data.next)
                data.next = data.start;

            service.send('insert', {collectionName : 'schedule', data : data}, function(err, doc) {

                if(err)
                    return res.error(err);

                res.send(201, {
                    createdAt : doc.data.createdAt,
                    objectId : doc.data.objectId
                });
            });
        });
    }).fail(function(err) {

        res.error(err);
    });
};

exports.read = function(req, res, container) {

    container.getService('MONGODB').then(function(service) {

        service.send('findOne', {collectionName : 'schedule', query : {where : {objectId : req.params._objectId}}}, function(err, doc) {

            if(err)
                return res.error(err);

            res.send(200, doc.data);
        });
    }).fail(function(err) {

        res.error(err);
    });
};

exports.update = function(req, res, container) {

    var data = req.data;

    if(!data)
        return res.error(new Error('RequestBodyNotFound'));

    container.getService('MONGODB').then(function(service) {

        service.send('update', {collectionName : 'schedule', query : {where : {objectId : req.params._objectId}}, data : data}, function(err, doc) {

            if(err) {

                if(err.code === 10147)
                    return new Error(404, 'ResourceNotFound');

                return res.error(err);
            }


            res.send(200, {
                updatedAt : doc.data.updatedAt
            });

        });
    }).fail(function(err) {

        res.error(err);
    });
};

exports.find = function(req, res, container) {

    container.getService('MONGODB').then(function (service) {

        if(!req.query.where)
            req.query.where = {};

        service.send('find', {collectionName : 'schedule', query: req.query}, function (err, docs) {

            if (err)
                return res.error(err);


            if (typeof(docs.data) === 'number') {

                res.send(200, {results: [], count: docs.data});
            } else {

                res.send(200, {results: docs.data});
            }
        });
    }).fail(function (err) {

        res.error(err);
    });
};

exports.destroy = function(req, res, container) {

    container.getService('MONGODB').then(function(service) {

        service.send('remove', {collectionName : 'schedule', query : {where : {objectId : req.params._objectId}}}, function(err, doc) {

            if(err)
                return res.error(err);

            res.send(200, {});
        });
    }).fail(function(err) {

        res.error(err);
    });
};

exports.destroyAll = function(req, res, container) {

    // 테스트에서만 사용
    if(process.env.NODE_ENV !== 'test') {

        return new Error("cannot access");
    }

    container.getService('MONGODB').then(function(service) {

        service.send('remove', {collectionName : 'schedule', query : {}}, function(err, doc) {

            if(err)
                return res.error(err);

            res.send(200, {});
        });
    }).fail(function(err) {

        res.error(err);
    });
};
