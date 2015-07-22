"use strict";

var _ = require('lodash');

exports.container = null;

var scheduleInterval = null;

exports.init = function(container, callback) {

    exports.container = container;

    container.addListener('request', onRequest);

    runScheduler();

    callback(null);
};

exports.close = function(callback) {

    if(scheduleInterval) {

        clearInterval(scheduleInterval);
    }

    callback(null);
};

exports.request = onRequest;

function onRequest(req, res) {

    var controller = require('./controllers/scheduleController');

    if(req.data.params._query1)
        req.data.params._objectId = req.data.params._query1;

    var checklist = ['APIAUTH', 'MASTERKEY'];

    var dest = getRouteDestination(req.data);

    if(!dest) {

        return res.error(new Error('ResourceNotFound'))
    }

    exports.container.getService('AUTH').then(function(service) {

        var reqData = {checklist : checklist};

        var deep = function(a, b) {
            return _.isObject(a) && _.isObject(b) ? _.assign(a, b, deep) : b;
        };

        service.send('check', _.assign(reqData, req.data, deep), function(err, response) {

            if(err) {

                return res.error(err);
            }

            setReqFromSession(req.data, response.data.session);

            controller[dest](req.data, res, exports.container);
        });

    }).fail(function(err) {

        res.error(new Error('auth service not found'));
    });
}

function setReqFromSession(reqData, session) {

    reqData.session = session;

    if(session.userid) {

        reqData.query.where._userid = session.userid;

        if(reqData.data)
            reqData.data._userid = session.userid;
    } else if(!session.masterKey){

        reqData.query.where._userid = { '$exists' : false }
    }

    if(session.appid) {

        reqData.query.where._appid = session.appid;

        if(reqData.data)
            reqData.data._appid = session.appid;
    }
}

function getRouteDestination(reqData) {

    var dest = '';

    switch(reqData.method) {

        case 'GET' :
            if(reqData.params._objectId)
                dest = 'read';
            else
                dest = 'find';
            break;

        case 'POST' :
            dest = 'create';
            break;

        case 'PUT' :
            dest = 'update';
            break;

        case 'DELETE' :
            if(reqData.params._objectId)
                dest = 'destroy';
            else
                dest = 'destroyAll';
            break;
    }

    return dest;
}

function runScheduler() {

    var interval = 10 * 60 * 1000;

    if(process.env.NODE_ENV === 'test')
        interval = 10000;

    scheduleInterval = setInterval( function() {

        require('./controllers/scheduler').run(exports.container);

    }, interval);
}
