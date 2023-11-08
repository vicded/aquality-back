const http = require('http');
const express = require('express');
const mysql = require('mysql');
const {Server} = require('socket.io');
const cors = require('cors');
const config = require('dotenv').config();

var beforeInsert = 0;

const app = express();

app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: "http://localhost:3000",
        methods: ["GET", "POST"],
    },
})

const db = mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB,
    timezone: 'Z'
});

io.on('connection', (socket) => {
    console.log('Client connected');
  
    socket.on('disconnect', () => {
      console.log('Client disconnected');
    });
});

function numberOfEvents() {
    return result = dbQuery('SELECT COUNT(*) as total FROM station_events WHERE DATE(date_entered) = ?');
}

function dbQuery(databaseQuery) {
    return new Promise(data => {
        let todayDate = new Date();
        console.log('date creation', todayDate);
        todayDate = todayDate.toISOString().split('T')[0];
        console.log('today', todayDate);
        //todayDate = '2023-09-06';
        console.log('TODAY DATEEEEe', todayDate);
        db.query(databaseQuery, todayDate, function (error, result) { // change db->connection for your code
            if (error) {
                console.log(error);
                throw error;
            }
            try {
                data(result);

            } catch (error) {
                data({});
                throw error;
            }
        });
    });
}

function getLatestEvent() {
    return new Promise (data => {
        db.query({sql: 'SELECT s.station_name, se.ph, se.ec, se.tds, se.turbidity, se.temperature, se.date_entered FROM station_events se JOIN stations s ON s.device_mac = se.mac ORDER BY se.date_entered DESC LIMIT 2', typeCast: true}, function(error, result) {
            if (error) {
                throw error;
            }
            try {
                data(result);
            } catch (error){
                data({});
                throw error;
            }
        });
    });
}

function getDayAverage(date) {
    return new Promise (data => {
        db.query({sql: 'SELECT AVG(se.ph) as ph_avg, AVG(se.ec) as ec_avg, AVG(se.tds) as tds_avg, AVG(se.turbidity) as turbidity_avg, AVG(temperature) as temperature_avg FROM station_events se WHERE DATE(date_entered) = ?', typeCast: true}, date, function(error, result) {
            if (error) {
                throw error;
            }
            try {
                data(result);
            } catch (error){
                data({});
                throw error;
            }
        });
    });
}

function getMonthlyAverage(month) {
    console.log('month method', month);
    return new Promise (data => {
        db.query({sql: 'SELECT AVG(se.ph) as ph_avg, AVG(se.ec) as ec_avg, AVG(se.tds) as tds_avg, AVG(se.turbidity) as turbidity_avg, AVG(temperature) as temperature_avg FROM station_events se WHERE MONTH(date_entered) = ?', typeCast: true}, month, function(error, result) {
            if (error) {
                throw error;
            }
            try {
                data(result);
            } catch (error){
                data({});
                throw error;
            }
        });
    });
}

function getEventsByDateRange(startDate, endDate) {
    console.log('start date', startDate);
    console.log('endDate', endDate);

    return new Promise (data => {
        db.query({sql: 'SELECT se.ph, se.ec, se.tds, se.turbidity, se.temperature, se.date_entered FROM station_events se WHERE DATE(se.date_entered) BETWEEN ? AND ? ORDER BY date_entered DESC;', typeCast: true}, [startDate, endDate], function(error, result) {
            if (error) {
                throw error;
            }
            try {
                data(result);
            } catch (error){
                data({});
                throw error;
            }
        });
    });
}


function emitLatestEvent(latestEvent) {
    io.emit('periodicData', latestEvent);
}

function parseQueryResult(queryObj) {
    if (queryObj) {
        return JSON.stringify(queryObj[0].total);
    } else {
        return '';
    }
}

async function pollInserts() {
    try {
        var resultsAfter = await numberOfEvents();
        resultsAfter = parseQueryResult(resultsAfter);
        console.log('Results After', resultsAfter);
        console.log('Results Before', beforeInsert);
        
        if (beforeInsert == 0) {
            var latestEvent = await getLatestEvent();
            latestEvent = JSON.stringify(latestEvent[0]);
            emitLatestEvent(latestEvent);
        }
        
        if (resultsAfter > beforeInsert) {
            console.log('-------------- INSERT DETECTED --------------');
            beforeInsert = resultsAfter;
            var latestEvent = await getLatestEvent();
            latestEvent = JSON.stringify(latestEvent[0]);
            console.log('LATEST EVENT: ', latestEvent);
            emitLatestEvent(latestEvent);
            console.log('-------------- INSERT END -------------------');
        } else {
            var latestEvent = await getLatestEvent();
            latestEvent = JSON.stringify(latestEvent[0]);
            emitLatestEvent(latestEvent);
        }

    }
    catch (error){
        console.error('There was an error while polling: ', error);
    }
}


app.get('/', async (req, res) => {
    const latestEvent = await getLatestEvent();
    
    console.log('latest', latestEvent);
    res.json(latestEvent[0]);
});

app.get('/daily_average', async (req, res) => {
    const date = req.query.date;
    console.log('date', date);
    const dailyAverage = await getDayAverage(date);

    console.log('latest', dailyAverage);
    res.json(dailyAverage[0]);
});

app.get('/monthly_average', async (req, res) => {
    const month = req.query.month;
    console.log('month', month);
    if (!month){
        return res.status(400).send({error: 'Invalid parameters'});
    }
    const monthlyAverage = await getMonthlyAverage(month);
    
    console.log('latest monthly', monthlyAverage);

    res.json(monthlyAverage[0])

})

app.get('/events', async (req, res) => {
    const startDate = req.query.start;
    const endDate = req.query.end;

    console.log('date', startDate, endDate);
    const events = await getEventsByDateRange(startDate, endDate);

    console.log('latest', events);
    res.json(events);
});

const port = 3001;

server.listen(port, () => {
    console.log(`Server is running on port ${port}`);
});

setInterval(pollInserts, 5000);

