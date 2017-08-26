'use strict';

const Rethink_Driver = require( '../index.js' );
const tape = require( 'tape-async' );

tape( 'API: imports properly', t => {
    t.ok( Rethink_Driver, 'module exports' );
    t.equal( Rethink_Driver && typeof Rethink_Driver.create, 'function', 'exports create()' );
    t.end();
} );

tape( 'API: API is correct on driver instance', t => {

    const rethink_driver = Rethink_Driver.create();

    t.ok( rethink_driver, 'got driver instance' );

    t.equal( rethink_driver && typeof rethink_driver.init, 'function', 'exports init' );
    t.equal( rethink_driver && typeof rethink_driver.put, 'function', 'exports put' );
    t.equal( rethink_driver && typeof rethink_driver.get, 'function', 'exports get' );
    t.equal( rethink_driver && typeof rethink_driver.del, 'function', 'exports del' );

    t.end();
} );