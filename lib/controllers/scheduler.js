"use strict";

require('date-utils');

exports.run = function(container) {

    container.getService('MONGODB').then(function (mongodbService) {

        var query = {
            next : {
                '$lt' : new Date()
            }
        };

        mongodbService.send('find', {collectionName : 'schedule', query: query}, function (err, docs) {

            if (err)
                return console.log(err.message);

            if(docs && docs.data && docs.data.length > 0) {

                for(var i= 0, cnt=docs.data.length; i<cnt; i++) {

                    var doc = docs.data[i];
                    var updateDoc = {};

                    // calc next

                    if(doc.repeat) {

                        if(doc.repeat.interval)
                            updateDoc.next = doc.next.addMinutes(doc.repeat.interval);

                    } else {

                        updateDoc.next = null;
                    }

                    // update

                    if(doc.count)
                        updateDoc.count = doc.count + 1;
                    else
                        updateDoc.count = 1;

                    mongodbService.send('update', {collectionName : 'schedule', query : {where : {objectId : doc.objectId}}, data : updateDoc}, function(err, doc) {

                        if(err) {

                            if(err.code === 10147)
                                return console.log('ResourceNotFound');

                            return console.log(err.message);
                        }
                    });

                    // run
                    if(doc.function) {

                        mongodbService.send('findOne', {collectionName : 'apps', query : {where : {objectId : doc._appid}}}, function(err, app) {

                            if(err)
                                return console.log(err.message);

                            container.getService('FUNCTIONS').then(function (functionService) {

                                functionService.send('run', {
                                    appid : doc._appid,
                                    functionName : doc.function,
                                    parameter : doc.parameter,
                                    applicationId : app.applicationId,
                                    javascriptKey : app.javascriptKey,
                                    masterKey : app.masterKey
                                }, function(err, doc) {

                                    if(err) {

                                        if(err.code === 10147)
                                            return console.log('ResourceNotFound');

                                        return console.log(err.message);
                                    }
                                });
                            }).fail(function (err) {

                                console.log(err.message);
                            });
                        });
                    }
                }
            }
        });
    }).fail(function (err) {

        console.log(err.message);
    });
};
