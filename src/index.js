var redisFactory = require( "./redis" );

var EventStore = require( "./EventStore" );
var ActorStore = require( "./ActorStore" );

var cache = {
	events: {},
	actors: {}
};

module.exports = function( _config ) {
	var config = _config || {};

	var redis = redisFactory( config.redis );

	return {
		events: {
			create: function( actorType, config ) {
				if ( cache.events[ actorType ] ) {
					return cache.events[ actorType ];
				}
				var store = new EventStore( redis, actorType, config );

				cache.events[ actorType ] = store;

				return store;
			}
		},
		actors: {
			create: function( actorType, config ) {
				if ( cache.actors[ actorType ] ) {
					return cache.actors[ actorType ];
				}
				var store = new ActorStore( redis, actorType, config );

				cache.actors[ actorType ] = store;

				return store;
			}
		},
		redis: redis
	};
};
