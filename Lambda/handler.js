import { Kafka } from 'kafkajs'
import fs from 'fs'

// Create Kafka Instance
const kafka = new Kafka({
    brokers: [process.env.KAFKA_BROKER],
    clientId: `Kinesis-Kafka-server`,
    ssl: {
        ca: [fs.readFileSync(path.join(__dirname, 'certs/kafka.pem'), 'utf-8')]
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
        producer.connect();
        console.log('Producer Connected');

        console.log('EVENT', event);

    } catch (error) {
        console.error('Error Occured', error)
    }
}