const express = require('express')
const mysql = require('mysql2/promise')
const MongoClient = require('mongodb').MongoClient // or const {MongoClient} = require('mongodb')


const APP_PORT = parseInt(process.argv.APP_PORT) || parseInt(process.env.APP_PORT) || 3000

const MONGO_URL = 'mongodb://localhost:27017'
const MONGO_DB = 'airbnb'
const MONGO_COLLECTION = 'listingsAndReviews'

//create function and run everything in it so that we can use async-await
const avgCleanliness = async (propertyType, client) => {    //parse in property_type and mongoclient
    const result = await client.db(MONGO_DB).collection(MONGO_COLLECTION).aggregate([
        {$match: 
            {property_type: propertyType }},
        {$group: {
            _id: '$address.country',  //group by country which is in address
            count: {$sum: 1}, //sum all entires in each grp
            total_cleanliness_score: {$push: '$review_scores.review_scores_cleanliness'} //push cleanliness score of each document into an array
        }},
        {$project: {
            avg_cleanliness: {$avg: '$total_cleanliness_score'}
        }},
        {$sort : {avg_cleanliness: 1}}

    ]).toArray()

    return result
}


const client = new MongoClient(MONGO_URL, {
    useNewUrlParser: true, useUnifiedTopology: true
})

client.connect() //returns a promise
    .then( async () => {

        //execute query here else u are trying to connect when there is no connection
        const result = await avgCleanliness('Condominium', client)

        //list out the results
        console.log("result = ", result)

        //stop the client when query done
        client.close()
    })