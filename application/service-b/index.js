import express from 'express'
import morgan from 'morgan'

const app = express();

const PORT = 9001;

// Middleware for morgan
app.use(morgan('common'))   // logs everything and sends them as stdout

app.get('/hello', (req,res) => {
    res.status(200).json({message: 'Hello from Service B'});
})

app.listen(PORT, () => {
    console.log(`Service B is running on port ${PORT}`);
})