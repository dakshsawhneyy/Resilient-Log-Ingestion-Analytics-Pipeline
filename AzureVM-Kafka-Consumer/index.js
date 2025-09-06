const express = require('express')
const { createClient } = require('@clickhouse/client')
const { Kafka } = require('kafkajs')
const { v4:uuid } = require('uuid')
const fs = require('fs')
const path = require('path')    // used to join path

require('dotenv').config()

// Create Kafka Client
const kafka = new Kafka({
    brokers: [process.env.KAFKA_BROKER],
    clientId: `kafka-consumer-server`,
    ssl: {
        ca: [fs.readFileSync(path.join(__dirname, 'kafka.pem'), 'utf-8')]
    },
    sasl: {
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD,
        mechanism: 'plain'
    }
})

// Create CLickhouse Client
const clickhouse = createClient({
    url: process.env.CLICKHOUSE_HOST,
    username: process.env.CLICKHOUSE_USER,
    password: process.env.CLICKHOUSE_PASSWORD,
    database: process.env.CLICKHOUSE_DATABASE,
})


// Creating Kafka Consumer
const consumer = kafka.consumer({ groupId: `kafka-consumer-server` })

const app = express();
const PORT = 11000;

// Function for kafka consumer
const kafkaConsumer = async() => {
    await consumer.connect();
    console.log(`Consumer Connected !!!`);

    // Subscribe to topic
    await consumer.subscribe({ topic: 'aiven-kafka' })
    console.log('Consumer subscribed to topic: `aiven-kafka`')

    // Run consumer
    await consumer.run({
        autoCommit: false,  // kafka automatically commit offset, but here we doing manually
        
        // Instead of processing them one by one, process them in batches
        eachBatch: async function({ batch, heartbeat, commitOffsetsIfNecessary, resolveOffset }) {
            const messages = batch.messages
            console.log(`Recieved Messages: ${JSON.stringify(messages)}`)

            // looping over messages
            for(const message of messages) {
                // Converting messgage into JS String from Buffer and then to JS Object
                const stringMessage = message.value.toString();
                const record = JSON.parse(stringMessage);
                
                // Extract parts, status, method and ts from record
                const parts = record.split("\t")
                const ts = new Date(parseFloat(parts[0]) * 1000)
                const status = parseInt(parts[2])
                const method = parts[3]
                const path = parts[4]

                // Insert logs into clickhouse
                try {
                    // Clickhouse returns an query_id for every insertion
                    const {query_id} = await clickhouse.insert({
                        table: 'cloudfront_logs',
                        values: [{ 
                            event_id: uuid(), 
                            timestamp: ts.toISOString().replace("T"," ").split(".")[0], // 'YYYY-MM-DD hh:mm:ss'
                            ip: parts[1],
                            status: status,
                            method: method,
                            url_path: path,
                            edge_location: parts[5],
                            user_agent: parts[6]
                        }],
                        format: 'JSONEachRow'
                    })
                    
                    // Offset Handling -- sequence no. in Kafka. It helps remember kafka where we had left off
                    await resolveOffset(message.offset)     // mark messages as processed (local)
                    await commitOffsetsIfNecessary(message.offset)    // telling Kafka: “I’m done with all messages up to this offset. You don’t need to send them again if I crash.”
                    await heartbeat();  // if consumer goes quiet for too long, kafka kicks it out of the group
                    
                    console.log(`${record} added to clickhouse successfully [${query_id}]`)
                } catch (error) {
                    console.error('Error inserting log into ClickHouse:', error)
                }
            }
        }

    })

}

kafkaConsumer()

app.listen(PORT, () => {
    console.log(`Listening on port: ${PORT}`)
})