'use strict';

const extend = require( 'extend' );
const RethinkDB = require( 'rethink-plus' );

const Rethink_Driver = {
    init: async function() {
        this.db = new RethinkDB( this.options.db );

        await this.db
            .dbList()
            .contains( this.options.database )
            .do( db_exists => {
                return RethinkDB.r
                    .branch(
                        db_exists,
                        { created: 0 },
                        RethinkDB.r.dbCreate( this.options.database )
                    );
            } );

        const table_options = extend( true, {
            primaryKey: this.options.id_field,
            shards: 1,
            replicas: 1
        }, this.options.table_options );

        await this.db
            .db( this.options.database )
            .tableList()
            .contains( this.options.table )
            .do( table_exists => {
                return RethinkDB.r
                    .branch(
                        table_exists,
                        { created: 0 },
                        RethinkDB.r.db( this.options.database ).tableCreate( this.options.table, table_options ) );
            } );
    },

    stop: async function() {
        this.db = null;
    },

    put: async function( object ) {
        const table = this.db.table( this.options.table );
        await table.insert( object, {
            conflict: 'replace'
        } );
    },

    get: async function( id ) {
        const table = this.db.table( this.options.table );
        const result = await table.get( id );
        return result;
    },

    del: async function( id ) {
        const table = this.db.table( this.options.table );
        await table.get( id ).delete();
    }
};

module.exports = {
    create: function( _options ) {
        const options = extend( true, {
            readable: true,
            id_field: 'id',
            db: {
                host: 'localhost',
                port: 28015
            },
            database: null,
            table: null
        }, _options );

        const instance = Object.assign( {}, Rethink_Driver );
        instance.options = options;

        return instance;
    }
};