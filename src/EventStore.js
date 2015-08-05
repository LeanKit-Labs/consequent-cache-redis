var util = require( "util" );
var _ = require( "lodash" );
var when = require( "when" );
var pipeline = require( "when/pipeline" );
var sliver = require( "./sliver.js" )();

/**
 * Storage mechanism for events
 * @constructor
 * @param {object} redis - Instance of Redis
 * @param {string} type - Name of event store
 * @params {object} _config - Configuration options for event store
*/

function EventStore( redis, type, _config ) {
	this.redis = redis;
	this.name = type;

	var config = _config || {};

	var eventPrefix = config.eventPrefix || util.format( "%s_events", this.name.toLowerCase() );
	var eventPackPrefix = config.eventPackPrefix || util.format( "%s_event_packs", this.name.toLowerCase() );

	this.getEventKey = function( key ) {
		return util.format( "%s/%s", eventPrefix, key );
	};

	this.getEventPackKey = function( key ) {
		return util.format( "%s/%s", eventPackPrefix, key );
	};
}

/**
 * Queries events for an actor since a given event id.
 * @param {string} aggregateId - The actor's id
 * @param {string} lastEventId - The lower bound for the event id query
 * @returns {array} Events since the last id, not including the last id
*/

EventStore.prototype.getEventsFor = function( aggregateId, lastEventId ) {
	var listKey = this.getEventKey( aggregateId + "/events" );
	var indexKey = this.getEventKey( aggregateId + "/index" );

	var getStartIndex = function() {
		return this.redis.hget( indexKey, lastEventId )
			.then( function( result ) {
				if ( _.isFinite( result ) ) {
					return result + 1;
				}
				return result;
			} );
	}.bind( this );

	var getEvents = function( startIndex ) {
		if ( !startIndex ) {
			return when( [] );
		}
		return this.redis.lrange( listKey, startIndex, -1 );
	}.bind( this );

	return pipeline( [
		getStartIndex,
		getEvents
	] ).then( function( results ) {
		return _.map( results, JSON.parse );
	} );
};

/**
 * Stores a list of events as individual records related to the actor id
 * @param {string} aggregatId - The related actor's id
 * @params {array} events - Collection of events to store
*/

EventStore.prototype.storeEvents = function( aggregateId, events ) {
	var doc;

	var listKey = this.getEventKey( aggregateId + "/events" );
	var indexKey = this.getEventKey( aggregateId + "/index" );

	var insert = function( events ) {
		return when.all( _.map( events, function( event ) {
			doc = {
				id: event.id || sliver.getId(),
				aggregateId: aggregateId, // jshint ignore:line
				event: event
			};

			var afterInsert = function( id, index ) {
				return { id: id, index: index };
			}.bind( undefined, doc.id );

			return this.redis.rpush( listKey, JSON.stringify( doc ) ).then( afterInsert );
		}.bind( this ) ) );
	}.bind( this );

	var indexer = function( insertResults ) {
		return when.all( _.map( insertResults, function( r ) {
			this.redis.hset( indexKey, r.id, r.index );
		}.bind( this ) ) ).then( function() {
			return insertResults;
		} );
	}.bind( this );

	return pipeline( [
		insert,
		indexer
	], events );
};

/**
 * Queries an event pack for a specific version of an actor.
 * @param {string} aggregateId - The actor's id
 * @param {string} vectorClock - Actor version
 * @returns {array} Events from the retrieved event pack
*/

EventStore.prototype.getEventPackFor = function( aggregateId, vectorClock ) {
	var key = this.getEventPackKey( aggregateId + "/" + vectorClock );

	var onSuccess = function( results ) {
		var events = _.map( results, JSON.parse );

		return _.sortBy( events, function( d ) {
			return d.id;
		} );
	};

	return this.redis.lrange( key, 0, -1 ).then( onSuccess );
};

/**
 * Stores a collection of events as a single record associated with a version of an actor
 * @param {string} aggregateId - The actor's id
 * @param {string} vectorClock - The actor's version
 * @param {array} events - Collection of events to store
*/

EventStore.prototype.storeEventPack = function( aggregateId, vectorClock, events ) {
	var key = this.getEventPackKey( aggregateId + "/" + vectorClock );

	var eventStrings = _.map( events, JSON.stringify );

	var args = [ key ].concat( eventStrings );

	return this.redis.rpush.apply( this.redis, args );
};

module.exports = EventStore;
