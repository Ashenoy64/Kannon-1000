const express = require('express');
const app = express();

// Arrays to store latency for calculations
let latencies = [];

// Middleware to track requests and responses
app.use((req, res, next) => {
    const start = Date.now();

    res.on('finish', () => {
        const end = Date.now();
        const latency = end - start;
        latencies.push(latency);
    });

    next();
});

// Endpoint for testing
app.get('/ping', (req, res) => {
    res.send('Pong!');
});

// Endpoint to view metrics
app.get('/metrics', (req, res) => {
    const totalRequests = latencies.length;
    const totalResponses = totalRequests; // Assuming every request gets a response

    const sum = latencies.reduce((acc, curr) => acc + curr, 0);
    const avgLatency = totalRequests > 0 ? sum / totalRequests : 0;

    const minLatency = Math.min(...latencies);
    const maxLatency = Math.max(...latencies);

    const sortedLatencies = [...latencies].sort((a, b) => a - b);
    const medianLatency = calculateMedian(sortedLatencies);

    res.json({
        requests: totalRequests,
        responses: totalResponses,
        averageLatency: avgLatency,
        minLatency: minLatency,
        maxLatency: maxLatency,
        medianLatency: medianLatency,
    });
});

// Function to calculate median
function calculateMedian(arr) {
    const length = arr.length;
    if (length === 0) return 0;

    const mid = Math.floor(length / 2);
    if (length % 2 === 0) {
        return (arr[mid - 1] + arr[mid]) / 2;
    } else {
        return arr[mid];
    }
}

// Start the server
const PORT = 3000; // You can change this to any port you prefer
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
