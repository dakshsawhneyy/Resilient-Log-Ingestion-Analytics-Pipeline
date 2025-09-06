import axios from 'axios'

export const handler = async(event) => {
    try {
        // console.log('EVENT', event);

        // console.log(event.Records[0].kinesis)
        const records = event.Records.map(record => {
            // Decoding base64 data, back into human readable format
            const payload = Buffer.from(record.kinesis.data, 'base64').toString('utf-8');
            return JSON.parse(payload);
        })

        // Send these data to EC2
        await axios.post("http://43.205.195.221:10000/data", { records });

        console.log(`Logs sent to EC2 successfully`);
    } catch (error) {
        console.error('Error Occured', error)
    }
}