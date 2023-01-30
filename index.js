const {Storage} = require('@google-cloud/storage');


exports.readObservation = (file , context ) => {
    const gcs = new Storage();
    const datafile = gcs.bucket(file.bucket).file(file.name);
    datafile.createReadStream()
    .on('error',  () => {
        console.log(`Error in stream: ${error}`);
    })
    .on('data', (row)  => {
        console.log(row);
    } )
    .on('end',() => {
        console.log('End of File');
    } )


  
    

}
