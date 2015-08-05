var _ = require( "lodash" );
var when = require( "when" );
var parallel = require( "when/parallel" );

function clockToId( actorId, clock ) {
	return actorId + "-" + ( _.isPlainObject( clock ) ? JSON.stringify( clock ) : clock );
}

/**
 * Storage mechanism for actors
 * @constructor
 * @param {object} db - Instance of Riaktive
 * @param {string} type - Name of actor store
 * @params {object} _config - Configuration options for actor store
*/

function ActorStore( db, type, _config ) {
	this.db = db;
	this.name = type;

	var config = _config || {};

	var actorBucketName = config.actorBucket || this.name.toLowerCase();

	var bucketConfig = {};

	this.actorBucket = this.db.bucket( actorBucketName, bucketConfig );
}

/**
 * Fetches a single instance of an actor. May return siblings.
 * @param {string} actorId
 * @returns {object|array} The actor object or the collection of siblings
*/

ActorStore.prototype.fetch = function( actorId ) {
	return this.actorBucket.get( actorId );
};

/**
 * Stores an instance of an actor
 * @param {string} actorId
 * @param {string} vectorClock ???
 * @params {object} actor
 * @returns {string} Created record id
*/

/*
	1. Set current actor "vector" to "ancestor"
	2. Increment current "vector"
	3. Fetch actor record from storage
	4. Create snapshot object with updated actor object (keyed by id & ancestor) (indexed by aggregateId)
	5. Update actor record using updated actor object
*/

ActorStore.prototype.store = function( actorId, vectorClock, _actor ) {
	var actor = _.cloneDeep( _actor );

	if ( !actor.id ) {
		actor.id = actorId;
	}

	actor.ancestor = actor.vector;
	actor.vector = vectorClock; // Assuming this is the clock that is passed in

	return this.actorBucket.get( actorId )
		.then( function( result ) {
			//if ( _.isArray( result ) ) {
			// You better figure out what to do with Riak siblings
			// Pick compatible sibling
			// result = _.find( result, function( r ) {
			// 	return !r.vector.isConcurrent( vectorClock );
			// });
			//}
			return result;
		}, function( err ) {
			return null;
		} ).then( function( result ) {
		var snapshot = _.cloneDeep( actor );
		var snapshotId = actorId + "-" + actor.vector;
		var snapshotIndexes = {
			aggregateId: actorId
		};

		if ( result && result.vclock ) {
			actor.vclock = result.vclock;
		}

		return when.all( [
			this.actorBucket.put( snapshotId, snapshot, snapshotIndexes ),
			this.actorBucket.put( actorId, actor )
		] );
	}.bind( this ) );
};

/**
 * Reconciles to a common ancestor between siblings
 * @param {string} actorId
 * @param {string} siblings
 * @returns {object} Actor object representing the ancestor
*/

ActorStore.prototype.findAncestor = function( actorId, siblings ) {
	/*
		Initialize ancestry object from sibling array.
		Example
			{
				sibling1Id: [ a1 ],
				sibling2Id: [ a2 ]
			}

		This entire workflow continues to construct the ancestry
		object by fetching each ancestor one by one until a common
		ancestor id is found in each array.
	*/
	var ancestry = _.reduce( siblings, function( memo, s ) {
			memo[ s.id ] = [];
			if ( s.ancestor ) {
				memo[ s.id ].push( s.ancestor );
			}
			return memo;
		}, {} );

	// Convert siblings array to object indexed by id for quick lookup
	var siblingIndex = _.indexBy( siblings, "id" );

	/*
		This callback extracts the arrays from the ancestry object.
		It maps over them to ensure all values are strings and
		then looks for an intersecting value.
	*/
	var compare = function( ancestry ) {
		var lists = _.map( _.values( ancestry ), function( list ) {
			return _.map( list, function( v ) {
				return JSON.stringify( v );
			} );
		} );

		var common = _.intersection.apply( null, lists );
		if ( _.isEmpty( common ) ) {
			return false;
		}
		return common;
	};

	/*
		Check to see if we already have a common ancestor from
		the information already available on siblings
	*/
	var alreadyHaveIt = compare( ancestry );

	if ( alreadyHaveIt ) {
		// If the common ancestor is already known, return it
		var lookupId = clockToId( actorId, alreadyHaveIt );
		return this.actorBucket.get( lookupId );
	} else {
		/*
			Kick off recursive function to load ancestors one level at a time
			until a common one is found
		*/
		return this._findAncestor( actorId, siblingIndex, ancestry, compare )
			.then( function( ancestor ) {
				var lookupId = clockToId( actorId, ancestor );
				return this.actorBucket.get( lookupId );
			}.bind( this ) );
	}
};

/*
	WHAT KIND OF LIMITS SHOULD WE PUT ON RECURSION TO PREVENT BAD DATA FROM BREAKING THE ENTIRE WORLD?
	WE COULD SET AN ARBITRARY LIMIT AND/OR ADD CYCLE DETECTION.
	IDEAS???
*/

ActorStore.prototype._findAncestor = function( actorId, siblings, ancestry, compareFn ) {
	/*
		This callback is run once per sibling during each phase
		It looks up the ancestor by key and pushes the next ancestor into the ancestry object
		If the record does not exist or if the ancestor has no ancestor of its own, this function
		pads the ancestry array with a 0 to signify it is closed.
	*/
	var taskFn = function( siblingId, key ) {
		return this.actorBucket.get( key )
			.then( function( ancestor ) {
				if ( ancestor.ancestor ) {
					ancestry[ siblingId ].push( ancestor.ancestor );
				} else {
					ancestry[ siblingId ].push( 0 );
				}
			}, function( err ) {
				ancestry[ siblingId ].push( 0 );
			} );
	};

	/*
		This function reduces the ancestry object to an array of tasks
		corresponding to the next level of ancestors that needs to be retrieved.
	*/
	var tasks = _.reduce( ancestry, function( memo, ancestorIds, siblingId ) {
		// Lookup the ancestor in the last index on the array
		var lookupId = _.last( ancestorIds );

		// Skip if there is no ancestor or if the array has been closed with 0
		if ( !lookupId || lookupId === 0 ) {
			return memo;
		}

		lookupId = clockToId( actorId, lookupId );

		var task = taskFn.bind( this, siblingId, lookupId );
		memo.push( task );
		return memo;
	}.bind( this ), [] );

	// Reject if no tasks have been created. We have reached the end of the line
	if ( _.isEmpty( tasks ) ) {
		return when.reject( new Error( "Common ancestor could not be found" ) );
	}

	return parallel( tasks )
		.then( function( results ) {
			// If common ancestor has been found, return it
			var commonAncestor = compareFn( ancestry );
			if ( commonAncestor ) {
				return commonAncestor;
			}
			// Otherwise, trigger another level up.
			return this._findAncestor( actorId, siblings, ancestry, compareFn );
		}.bind( this ) );
};

module.exports = ActorStore;
