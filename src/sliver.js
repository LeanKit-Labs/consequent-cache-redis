var sliver = require( "sliver" );
var instance;

module.exports = function( _config ) {
	var config = _config || {};

	var seed = config.seed || "seed string";

	if ( instance ) {
		return instance;
	}

	instance = sliver( seed );

	return instance;
};
