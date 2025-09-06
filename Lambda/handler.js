import axios from 'axios'

export const handler = async(event) => {
    try {
        // console.log('EVENT', event);

        const records = event.Records.map(record => {
            // Decode base64 payload
            const payload = Buffer.from(record.kinesis.data, 'base64').toString('utf-8').trim();
            // Return raw log line instead of JSON.parse
            return payload;
        });
        

        if (records.length === 0) {
            console.log('No valid records to send');
            return;
        }

        // Send these data to EC2
        await axios.post("http://43.205.195.221:10000/data", { data: records });

        console.log(`Logs sent to EC2 successfully`);
    } catch (error) {
        console.error('Error Occured', error)
    }
}