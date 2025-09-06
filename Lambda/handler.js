import axios from 'axios'

export const handler = async(event) => {
    try {
        // console.log('EVENT', event);

        // console.log(event.Records[0].kinesis)
        const records = event.Records.flatMap(record => {
            const payload = Buffer.from(record.kinesis.data, 'base64').toString('utf-8').trim();
            // Split by newline if multiple JSON objects exist
            return payload.split('\n').map(line => {
                try {
                    return JSON.parse(line);
                } catch (e) {
                    console.error('Invalid JSON line:', line, e);
                    return null;
                }
            }).filter(Boolean);
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