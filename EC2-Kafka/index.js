const { Kafka } = require('kafkajs')
const path = require('path')    // used to join path
const fs = require('fs')
const express = require('express')

// Create Kafka Instance
const kafka = new Kafka({
    brokers: [process.env.KAFKA_BROKER],
    clientId: `Kinesis-Kafka-server`,
    ssl: {
        ca: [fs.readFileSync(path.join(__dirname, 'kafka.pem'), 'utf-8')]
    },
    sasl: {
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD,
        mechanism: 'plain'
    }
})


const app = express();
const PORT = 10000;


// Creating Kafka Producer
const producer = kafka.producer();


// Fetching Data on /data route
app.use('/data', (req,res) => {
    const { data } = req.body;
    console.log(data);
})


app.listen(PORT, () => {
    console.log(`Listening on port: ${PORT}`)
})