const {Storage} = require('@google-cloud/storage');
const csv = require('csv-parser');
const fs = require('fs');
const {BigQuery} = require('@google-cloud/bigquery');
const bq = new BigQuery();
const datasetId = 'weather_etl';
const tableId = 'observations';

exports.readObservation = (file, context) => {
  const gcs = new Storage();
  const datafile = gcs.bucket(file.bucket).file(file.name);
  datafile.createReadStream()
  .pipe(csv())

  .on('error', () => {
    console.log(` Error in stream: ${error}`);
  })
  .on('data', (row) => {
    transformObject(row);
    writeToBq(row);

  })
  .on('end', () => {
    console.log('End of file reached.');
  })
}
function transformObject(obs) {
    // The .map() function will let us examine each element of the object one at a time
    // "key" is the name of the key (i.e., "airtemp")
    // "obs[key]" is the value of the element named in "key"
  
    Object.keys(obs).map( (key) => {
      
     
      if ( obs[key] == -9999){
          obs[key] = null;
          console.log(key+" "  + "NULL " );
          
      }
   else if (key == "year" || key == "month" || key == "day"  || key == "hour" || key == "sky" || key == "winddirection"  ){
       parseInt(obs[key]);
       console.log(key+" "  +obs[key]);
       
   }
   else if( key == "station"){
      obs[key] = '724380-93819'; 
      console.log(key+" "  +obs[key]);
    
  }else if( key == "airtemp" || key == "dewpoint" || key == "pressure"  || key == "windspeed" || key == "precip1hour" || key == "precip6hour" ){
      parseFloat(obs[key]);
      result = obs[key]/10;
      obs[key] = result;
      console.log(key+" "  + obs[key]);
     
  }
    });
   
  }
  
async function writeToBq(obs) {
    // BQ expects an array of objects, but we only have 1
    // Make an array with a single element
    var rows = [];
    rows.push(obs);
  
    // Insert data into the observations table
    await bq
      .dataset(datasetId)
      .table(tableId)
      .insert(rows)
      .then( () => {
        rows.forEach ( (row) => {`Inserted: ${row}`} )
      })
      .catch( (err) => { console.log(`ERROR: ${err}`)})
  }