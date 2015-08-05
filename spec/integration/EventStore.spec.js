require( "../setup.js" );

var when = require( "when" );
var _ = require( "lodash" );
var adapter;
var redis;
var sliver;

describe( "Event Store Interface", function() {
	before( function() {
		adapter = require( "../../src/index.js" )( config );
		redis = adapter.redis;

		sliver = require( "../../src/sliver.js" )();
	} );

	describe( "when storing events", function() {
		var store;
		var aggId;
		var listKey;
		var indexKey;
		var cachedList;
		var cacheIndex;
		var events = [
			{ name: "card1" },
			{ name: "card2" },
			{ description: "abc123" }
		];
		var ids;
		var records;
		before( function( done ) {
			aggId = sliver.getId();
			store = adapter.events.create( "card", {} );
			listKey = store.getEventKey( aggId + "/events" );
			indexKey = store.getEventKey( aggId + "/index" );

			// Beware the pyramid of doom
			store.storeEvents( aggId, events )
				.then( function( results ) {
					records = results;

					redis.lrange( listKey, 0, -1 )
						.then( function( list ) {
							cachedList = _.map( list, JSON.parse );

							redis.hgetall( indexKey )
								.then( function( res ) {
									cacheIndex = res;
									done();
								} );
						} );
				} );
		} );

		after( function( done ) {
			var rmList = redis.del( listKey );
			var rmIndex = redis.del( indexKey );

			when.all( [ rmList, rmIndex ] )
				.then( function() {
					done();
				} );
		} );

		it( "should save a document for each event", function() {
			_.pick( cachedList[ 0 ], "aggregateId", "event" ).should.eql( {
				aggregateId: aggId,
				event: events[ 0 ]
			} );

			_.pick( cachedList[ 1 ], "aggregateId", "event" ).should.eql( {
				aggregateId: aggId,
				event: events[ 1 ]
			} );

			_.pick( cachedList[ 2 ], "aggregateId", "event" ).should.eql( {
				aggregateId: aggId,
				event: events[ 2 ]
			} );
		} );

		it( "should index on the aggregate id", function() {
			_.values( cacheIndex ).length.should.equal( events.length );
			_.forOwn( cacheIndex, function( index, key ) {
				_.findIndex( cachedList, function( e ) {
					return e.id === key;
				} ).should.equal( index - 1 );
			} );
		} );
	} );

	describe( "when retrieving events", function() {
		var store;
		var aggId;
		var events;
		var ids;
		var records;
		before( function( done ) {
			aggId = sliver.getId();
			events = [
				{ name: "card1", id: sliver.getId() },
				{ name: "card2", id: sliver.getId() },
				{ description: "abc123", id: sliver.getId() },
				{ lane: "lane123", id: sliver.getId() },
				{ title: "a new title", id: sliver.getId() }
			];
			ids = _.pluck( events, "id" );
			store = adapter.events.create( "card", {} );
			store.storeEvents( aggId, events )
				.then( function() {
					return store.getEventsFor( aggId, ids[ 2 ] );
				} )
				.then( function( results ) {
					records = results;
					done();
				} );
		} );

		after( function( done ) {
			var listKey = store.getEventKey( aggId + "/events" );
			var indexKey = store.getEventKey( aggId + "/index" );

			when.all( [
				adapter.redis.del( listKey ),
				adapter.redis.del( indexKey )
			] ).then( function() {
					done();
				} );
		} );

		it( "should return the correct events", function() {
			records.length.should.equal( 2 );
			records.should.eql( [ {
					id: events[3].id,
					aggregateId: aggId,
					event: events[3]
				},
				{
					id: events[4].id,
					aggregateId: aggId,
					event: events[4]
				}
			] );
		} );
	} );

	describe( "when storing event packs", function() {
		var store;
		var aggId;
		var events = [
			{ name: "card1" },
			{ name: "card2" },
			{ description: "abc123" }
		];
		var clock;
		var key;
		var storedEvents;
		before( function( done ) {
			aggId = sliver.getId();
			clock = sliver.getId();
			store = adapter.events.create( "card", {} );
			key = store.getEventPackKey( aggId + "/" + clock );

			store.storeEventPack( aggId, clock, events )
				.then( function() {
					store.redis.lrange( key, 0, -1 )
						.then( function( result ) {
							storedEvents = _.map( result, JSON.parse );
							done();
						} );
				} );
		} );

		after( function() {
			return store.redis.del( key );
		} );

		it( "should save a document with embedded events", function() {
			storedEvents.should.eql( events );
		} );
	} );

	describe( "when retrieving event packs", function() {
		var store;
		var aggId;
		var events;
		var id;
		var records;
		var clock;
		var key;
		before( function( done ) {
			events = [
				{ name: "card1", id: sliver.getId() },
				{ name: "card2", id: sliver.getId() },
				{ description: "abc123", id: sliver.getId() }
			];
			aggId = sliver.getId();
			clock = sliver.getId();
			store = adapter.events.create( "card", {} );
			key = store.getEventPackKey( aggId + "/" + clock );

			store.storeEventPack( aggId, clock, events )
				.then( function() {
					store.getEventPackFor( aggId, clock )
						.then( function( res ) {
							records = res;
							done();
						} );
				} );
		} );

		after( function() {
			store.redis.del( key );
		} );

		it( "should return the list of events", function() {
			records.should.eql( events );
		} );
	} );
} );
