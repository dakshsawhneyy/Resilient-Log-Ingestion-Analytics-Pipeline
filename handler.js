import { Kafka } from 'kafkajs'
import fs from 'fs'
import path from 'path'


// Decoding the string back to PEM format
const caCert = Buffer.from(process.env.KAFKA_CA_BASE64, 'base64').toString('utf-8');

// Create Kafka Instance
const kafka = new Kafka({
    brokers: [process.env.KAFKA_BROKER],
    clientId: `Kinesis-Kafka-server`,
    ssl: {
        ca: [caCert]
    },
    sasl: {
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD,
        mechanism: 'plain'
    }
})

// Creating Kafka Producer
const producer = kafka.producer();

export const handler = async(event) => {
    try {
        
        // Connect Producer
        await producer.connect();
        console.log('Producer Connected');

        console.log('EVENT', event);

    } catch (error) {
        console.error('Error Occured', error)
    }
}