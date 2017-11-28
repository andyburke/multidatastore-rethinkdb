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

        const table_creation_result = await this.db
            .db( this.options.database )
            .tableList()
            .contains( this.options.table )
            .do( table_exists => {
                return RethinkDB.r
                    .branch(
                        table_exists,
                        { tables_created: 0 },
                        RethinkDB.r.db( this.options.database ).tableCreate( this.options.table, table_options ) );
            } );

        if ( table_creation_result.tables_created ) {
            // if we had to create the table, wait for it to be ready
            await this.db
                .db( this.options.database )
                .table( this.options.table )
                .wait();
        }

        const indexes = this.options.indexes || [];
        for ( const index of indexes ) {
            const index_creation_result = await this.db
                .db( this.options.database )
                .table( this.options.table )
                .indexList()
                .contains( index.name )
                .do( index_exists => {
                    return RethinkDB.r
                        .branch(
                            index_exists,
                            { created: 0 },
                            RethinkDB.r.db( this.options.database ).table( this.options.table ).indexCreate( index.name, index.create )
                        );
                } );

            if ( index_creation_result.created ) {
                await this.db
                    .db( this.options.database )
                    .table( this.options.table )
                    .indexWait( index.name );
            }
        }
    },

    stop: async function() {
        this.db && this.db.pool && typeof this.db.pool.drain === 'function' && this.db.pool.drain();
    },

    put: async function( object ) {
        const table = this.db.db( this.options.database ).table( this.options.table );
        await table.insert( object, {
            conflict: 'replace'
        } );
    },

    get: async function( id ) {
        const table = this.db.db( this.options.database ).table( this.options.table );
        const object = await table.get( id );
        return object;
    },

    del: async function( id ) {
        const table = this.db.db( this.options.database ).table( this.options.table );
        await table.get( id ).delete();
    }
};

module.exports = {
    create: function( _options ) {
        const options = extend( true, {}, {
            readable: true,
            id_field: 'id',
            db: {
                host: 'localhost',
                port: 28015
            },
            database: null,
            table: null,
            processors: []
        }, _options );

        const instance = Object.assign( {}, Rethink_Driver );
        instance.options = options;
        instance.r = RethinkDB.r;

        return instance;
    }
};