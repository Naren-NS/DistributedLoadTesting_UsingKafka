const express = require('express');
const app = express();
const PORT = process.env.PORT || 5000;

// Middleware to parse JSON bodies
app.use(express.json());

// Define /start_test endpoint
app.post('/start_test', (req, res) => {
    const testConfig = req.body;
    console.log('Starting test with config:', testConfig);
    res.json({ message: 'Test started successfully', config: testConfig });
});

app.post('/stop_test', (req, res) => {
    res.json({ message: 'Test stopped successfully' });
});

app.get('/metrics', (req, res) => {
    const metrics = {
        "total_requests": 100,
        "average_latency": 50,
        "max_latency": 100,
        "min_latency": 10
    };
    res.json(metrics);
});

app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});
