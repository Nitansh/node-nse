import { MongoClient } from "mongodb";
import fetch from "node-fetch";

const mongo_uri = "mongodb+srv://admin:daredevil@cluster0.ypmen.mongodb.net/";
const db_name = 'nse';
const collection_name = 'stocks';
const STEP = 10;
const BASE_URL = 'http://127.0.0.1:5000/'
const API_URL = {
  ALL_STOCK : 'all_stock/',
  SINGLE_STOCK : 'stock/'
};

const client = new MongoClient(mongo_uri, { useNewUrlParser: true });

async function connect( db_name, collection_name ) {
  await client.connect();
  return client.db( db_name ).collection( collection_name );
}

async function updateData( data, collection ) {
  const filter = { symbol: data['symbol'] };
  const options = { upsert: true };
  const updateDoc = {
    $set: {
      data : data
    },
  };
  const result = await collection.updateOne(filter, updateDoc, options);
  process.stdout.write('.');
}

const getScript = (url) => {
  let settings = { method: "Get" };
  return new Promise( ( resolve, reject ) => {
    fetch(url, settings)
    .then( res => resolve(
        res.json()
      ) )
  } );
};

// fetch all stock and and connect to DB in a promise.all
// iterate over all stock in 10 pieces to get it done
function fetchAndUpdate( stockList, collectionObject ) {
  try {
    getScript( `${ BASE_URL }${ API_URL['SINGLE_STOCK'] }`+stockList )
    .then( stocksData => {
      stocksData.forEach( stockData => {
        updateData( stockData, collectionObject );
      });
    } );
  } catch (error) {
    console.log( error );
  }
}

function updateStocks( allStocks, collectionObject ){
  if ( !checkTime() ){
    return;
  }
  for ( var i = 0; i < allStocks.length; i = i+STEP ) {
    fetchAndUpdate( allStocks.slice( i, i+STEP ).join('-'), collectionObject );
  }
}

function checkTime() {
  var d = new Date(); // current time
  var hours = d.getHours();
  var mins = d.getMinutes();
  var day = d.getDay();

  return day >= 1
      && day <= 5
      && ( hours >= 9 || hours === 9 && mins >= 15 )
      && (hours < 15 || hours === 15 && mins <= 30);
}

Promise.all([
  connect( db_name, collection_name ),
  getScript( `${ BASE_URL }${ API_URL['ALL_STOCK'] }`),
]).then(
  ( [
      collectionObject,
      allStocks,
  ] ) => {
    allStocks.shift();
    setInterval( ()=> updateStocks( allStocks, collectionObject ), 3*60*1000 )
  }
)