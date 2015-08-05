var redis = require( "redis" );
var _ = require( "lodash" );
var lift = require( "when/node" ).lift;

var commands = [ "rpush", "hset", "del", "get", "lrange", "hgetall", "hget" ];

module.exports = function( config ) {
	var _config = config || {};
	var client = redis.createClient( _config );

	_.each( commands, function( c ) {
		client[ c ] = lift( client[c] );
	} );

	return client;
};
