const { createClient } = require('@clickhouse/client')
const axios = require('axios')

require('dotenv').config()

// ClickHouse Client
const clickhouse = createClient({
    url: process.env.CLICKHOUSE_HOST,
    username: process.env.CLICKHOUSE_USER,
    password: process.env.CLICKHOUSE_PASSWORD,
    database: process.env.CLICKHOUSE_DATABASE,
})

// Function to send alert to slack
const sendAlertSlack = async(message) => {
    await axios.post(process.env.SLACK_WEBHOOK, { text: message })
}

// Function to fetch count of errors from clickhouse
const checkErrors = async() => {
    // Using the explicit subtractMinutes() function for maximum compatibility
    const query = `
        SELECT count(*) AS errors 
        FROM cloudfront_logs 
        WHERE status >= 500 
        AND timestamp > subtractMinutes(now(), 1)
    `;

    const resultSet = await clickhouse.query({ query, format: 'JSONEachRow' });
    // console.log('ResultSet: ', resultSet);

    // Convert DataSet to JSON
    const data = await resultSet.json();
    // console.log('DataSet: ', data)

    // If no errors are found, data array will be empty. // We need to handle this case to avoid an error.
    const errors = data.length > 0 ? data[0].errors : 0;
    // console.log('Errors: ', errors)

    if(errors > 0){
        await sendAlertSlack(`🚨 ALERT: ${errors} server errors in last 1 min!`)
        console.log('Alert sent to slack successfully')
    }
}

// Check again after one minute
const main = async() => {
    while(true){
        try {
            await checkErrors();
            console.log('checkError function ran successfully')
        } catch (error) {
            console.error('Error Occurred', error)
        }

        // Sleep for 1 minute
        console.log("Sleeping for 1 minute")
        await new Promise(res => setTimeout(res, 60000));
    }
}

main();