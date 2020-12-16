const express = require('express')
const morgan = require('morgan')
const mysql = require('mysql2/promise')
const MongoClient = require('mongodb').MongoClient // or const {MongoClient} = require('mongodb')
require('dotenv').config()


const APP_PORT = parseInt(process.argv.APP_PORT) || parseInt(process.env.APP_PORT) || 3000
const app = express()
app.use(morgan('combined'))

const MONGO_URL = 'mongodb://localhost:27017'
const MONGO_DB = 'bgg_comments'
const MONGO_COLLECTION = 'comments'

//config databases
//MYSQL create connection pool
const pool = mysql.createPool({
    host: process.env.MYSQL_SERVER_HOST,
    port: process.env.MYSQL_SERVER_PORT,
    user: process.env.MYSQL_USERNAME,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_SCHEMA,
    connectionLimit: process.env.MYSQL_CONN_LIMIT,
    timezone: '+08:00'
})

//start connection to MySQL
const startMYSQL = async (pool) => {
    
    const conn = await pool.getConnection()
    try{
        await conn.ping()
    }
    catch (e){
        console.error(`cannot ping MySQL: ${e}`)
    }finally{
        conn.release()
    }

}

//MongoDB create connection
const client = new MongoClient(MONGO_URL, {
    useNewUrlParser: true, useUnifiedTopology: true
})
const startMongo = client.connect()  //connect to mongoDB - have to connect to mongo first then can ping (but we dun really use ping in mongodb)


//mkSQLQuery
const mkSQLQuery = (sqlStmt, pool) => {
    return (async (args) => {
        const conn = await pool.getConnection()
        try{
            let results = await conn.query(sqlStmt, args || [])
            return results[0]

        }catch(err){
            console.error(` SQL Query error: ${err}`)
        }
        finally{
            conn.release()
        }
    })
}

//SQL Stmts
const SQL_GET_DETAILS = 'select name, year, url, image from game where gid=?;'
const getGameDetailsFromSQL = mkSQLQuery(SQL_GET_DETAILS, pool)

//GET /game/:id  - return {name, year, url, image, reviews: review_id[], average_rating} of a single game

app.get('/game/:id', async (req, resp) => {
    const game_id = parseInt(req.params.id)

    const resultFromSQL = await getGameDetailsFromSQL([game_id])
    console.log("results from sql ", resultFromSQL)

    const resultFromMongo = await client.db(MONGO_DB).collection(MONGO_COLLECTION).aggregate([
        {$match: {ID: game_id}}, 
        {$limit: 100}, //limit to 100 documents
        {$project: {_id: 1, name: 1, rating : 1}}, //get only review_id and rating of all reviews
        {$group: {
        _id: '$name',
        reviews: {$push:'$_id'},
        avg_rating: {$avg: '$rating'}}}
    ]).toArray()
    console.log("result from Mongo: ", resultFromMongo)

    // const [game, reviews] = await 
    Promise.all([resultFromSQL, resultFromMongo])
        .then(result => {
            const [game , reviews] = result
            resp.status(200).type('application/json')
            console.log("promise results: ", result)
            resp.json({
                name: game[0].name,
                year: game[0].year,
                url: game[0].url,
                image: game[0].image,
                reviews: reviews[0].reviews,
                average_rating: reviews[0].avg_rating
            })
        }).catch(e => {
            console.error("Error retrieving data: ", e)
            resp.status(500).type('application/json')
            resp.json({error: e})
        })
    
    

})












//sometimes u want to run a piece of code(and that code or function will only be used once)
// there is a way of declaring and running a function immediately - IIFE (immediately invoke function expression)
//eg const p2 = ((a, b) => {//do smth})('barney', 'fred') - the brackets at the end are to invoke the function immediately

/* const p0 = (async() =>{
    const conn = await pool.getConnection()
    await conn.ping()
    conn.release()
    //if u dun return anything it will still work as it will return undefine
})()

const p1 = (async() => {
    await client.connect()
    return true
})() */

//start server - check databases are up before starting server

//Promise.all([p0, p1])
Promise.all([startMYSQL(pool), startMongo])
    .then(result => {
        app.listen(APP_PORT, () => {
        console.info(`Application started on port ${APP_PORT} at ${new Date()}`)
        })
    }).catch( err =>
        console.error("error connecting to databases: ", err)
    )

