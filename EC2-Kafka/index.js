const { Kafka } = require('kafkajs')
const path = require('path')    // used to join path
const fs = require('fs')
const express = require('express')

require('dotenv').config();

console.log(process.env.KAFKA_USERNAME)
console.log(process.env.KAFKA_PASSWORD)
console.log(process.env.KAFKA_BROKER)

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

app.use(express.json());

const producer = kafka.producer();

const startServer = async() => {
    try {
        // Connect Kafka Producer
        await producer.connect();
        console.log('Connected to kafka producer')

        // Start server after producer is connected
        app.listen(PORT, () => {
            console.log(`Listening on port: ${PORT}`)
        })
    } catch (error) {
        console.error('Error Occured', error);
        process.exit(1);        
    }
}

startServer();

// Fetching Data on /data route
app.post('/data', async(req,res) => {
    try {
        const { data } = req.body;
        if(!data || data.length == 0) return res.status(500).send({ status: 'No Data Recieved' })

        // Store all messages into list
        const kafkaMessages = data.map(record => ({
            key: record.id || 'default',
            value: JSON.stringify(record),
        }))

        // Send this list to kafka topic
        await producer.send({
            topic: `aiven-kafka`,
            messages: kafkaMessages
        })
        console.log('Sent logs to topic `aiven-kafka`')


        res.status(200).send({status: 'Recvd and Send to Kafka', count: kafkaMessages.length})
    } catch (error) {
        console.error('Error occured:', error)   
        res.status(500).send({status: 'Failed Sending to Kafka'})
    }
})

