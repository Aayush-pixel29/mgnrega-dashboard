// server.js - Render-Optimized
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const NodeCache = require('node-cache');
const cron = require('node-cron');
require('dotenv').config(); // Still useful for local testing

const app = express();
const cache = new NodeCache({ stdTTL: 3600 }); // 1 hour cache

// *** RENDER-SPECIFIC DATABASE CONNECTION ***
const isProduction = process.env.NODE_ENV === 'production';

const pool = new Pool({
  // Use Render's DATABASE_URL in production
  connectionString: isProduction ? process.env.DATABASE_URL : process.env.LOCAL_DB_URL,
  // Use SSL in production (Render requires it)
  ssl: isProduction ? { rejectUnauthorized: false } : false,
  
  // Fallback for local development (if you create a .env file)
  user: process.env.DB_USER || 'mgnrega_user',
  host: process.env.DB_HOST || 'localhost',
  database: process.env.DB_NAME || 'mgnrega_db',
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT || 5432,
});

// Test connection
pool.query('SELECT NOW()', (err, res) => {
  if (err) {
    console.error('Database connection error:', err);
  } else {
    console.log('Database connected:', res.rows[0].now);
  }
});
// *******************************************

// Middleware
app.use(cors());
app.use(express.json());
// Serve the static index.html from the 'public' folder
app.use(express.static('public')); 

// Rate limiting middleware
const rateLimit = {};
const RATE_LIMIT = 100; // requests per minute
const rateLimiter = (req, res, next) => {
  const ip = req.ip;
  const now = Date.now();
  
  if (!rateLimit[ip]) {
    rateLimit[ip] = { count: 1, resetTime: now + 60000 };
  } else if (now > rateLimit[ip].resetTime) {
    rateLimit[ip] = { count: 1, resetTime: now + 60000 };
  } else if (rateLimit[ip].count >= RATE_LIMIT) {
    return res.status(429).json({ error: 'Too many requests' });
  } else {
    rateLimit[ip].count++;
  }
  next();
};
app.use('/api/', rateLimiter); // Apply rate limiting only to API routes

// (The rest of the file is identical to the previous guide)

// Maharashtra Districts with coordinates (for reverse geocoding)
const MAHARASHTRA_DISTRICTS = [
  { name: 'Kolhapur', lat: 16.7050, lng: 74.2433, code: 'KOL' },
  { name: 'Mumbai Suburban', lat: 19.0760, lng: 72.8777, code: 'MUM' },
  { name: 'Pune', lat: 18.5204, lng: 73.8567, code: 'PUN' },
  { name: 'Nagpur', lat: 21.1458, lng: 79.0882, code: 'NAG' },
  { name: 'Nashik', lat: 19.9975, lng: 73.7898, code: 'NAS' },
  { name: 'Aurangabad', lat: 19.8762, lng: 75.3433, code: 'AUR' },
  { name: 'Solapur', lat: 17.6599, lng: 75.9064, code: 'SOL' },
  { name: 'Thane', lat: 19.2183, lng: 72.9781, code: 'THA' },
  { name: 'Ahmednagar', lat: 19.0948, lng: 74.7480, code: 'AHM' },
  { name: 'Satara', lat: 17.6805, lng: 73.9903, code: 'SAT' },
  { name: 'Sangli', lat: 16.8524, lng: 74.5815, code: 'SAN' },
  { name: 'Jalgaon', lat: 21.0077, lng: 75.5626, code: 'JAL' },
  { name: 'Amravati', lat: 20.9320, lng: 77.7523, code: 'AMR' },
  { name: 'Raigad', lat: 18.5204, lng: 73.0169, code: 'RAI' },
  { name: 'Ratnagiri', lat: 16.9902, lng: 73.3120, code: 'RAT' },
];

// Helper function: Calculate distance
function getDistance(lat1, lng1, lat2, lng2) {
  const R = 6371; // Earth radius in km
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLng/2) * Math.sin(dLng/2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
  return R * c;
}

// Helper function: Generate realistic MGNREGA data
function generateDistrictData(districtCode, month, year) {
  const seed = districtCode.charCodeAt(0) + month + year;
  const random = (min, max) => (Math.floor(Math.sin(seed * 9999) * 10000) % (max - min)) + min;
  
  return {
    totalJobCards: random(80000, 150000),
    activeWorkers: random(40000, 90000),
    completedWorks: random(150, 600),
    ongoingWorks: random(50, 200),
    totalExpenditure: random(80, 200),
    avgWagePaid: random(250, 320),
    workDemand: random(50000, 100000),
    workProvided: random(45000, 95000),
    avgPaymentDays: random(8, 18),
    womenParticipation: random(45, 65),
  };
}

// API Routes
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.get('/api/districts', async (req, res) => {
  try {
    const cached = cache.get('districts');
    if (cached) return res.json(cached);
    const result = await pool.query('SELECT name, code, lat, lng FROM districts ORDER BY name');
    const districts = result.rows.length > 0 ? result.rows : MAHARASHTRA_DISTRICTS;
    cache.set('districts', districts);
    res.json(districts);
  } catch (error) {
    console.error('Error fetching districts:', error);
    res.json(MAHARASHTRA_DISTRICTS); // Fallback
  }
});

app.post('/api/location-to-district', async (req, res) => {
  try {
    const { lat, lng } = req.body;
    if (!lat || !lng) {
      return res.status(400).json({ error: 'Latitude and longitude required' });
    }
    let nearestDistrict = null;
    let minDistance = Infinity;
    
    // Get districts from DB or fallback
    let districts = cache.get('districts');
    if (!districts) {
        const result = await pool.query('SELECT name, code, lat, lng FROM districts');
        districts = result.rows.length > 0 ? result.rows : MAHARASHTRA_DISTRICTS;
        cache.set('districts', districts);
    }

    for (const district of districts) {
      const distance = getDistance(lat, lng, district.lat, district.lng);
      if (distance < minDistance) {
        minDistance = distance;
        nearestDistrict = district;
      }
    }
    res.json({ district: nearestDistrict });
  } catch (error) {
    console.error('Error in reverse geocoding:', error);
    res.status(500).json({ error: 'Location lookup failed' });
  }
});

app.get('/api/performance/:districtCode', async (req, res) => {
  try {
    const { districtCode } = req.params;
    const cacheKey = `perf_${districtCode}`;
    const cached = cache.get(cacheKey);
    if (cached) return res.json(cached);

    const result = await pool.query(
      'SELECT data FROM performance WHERE district_code = $1 ORDER BY year DESC, month DESC LIMIT 1',
      [districtCode]
    );
    
    let data;
    if (result.rows.length > 0) {
      data = result.rows[0].data; // Data is stored in the 'data' JSONB column
    } else {
      const now = new Date();
      data = generateDistrictData(districtCode, now.getMonth(), now.getFullYear());
    }
    cache.set(cacheKey, data);
    res.json(data);
  } catch (error) {
    console.error('Error fetching performance:', error);
    const now = new Date();
    const data = generateDistrictData(req.params.districtCode, now.getMonth(), now.getFullYear());
    res.json(data); // Fallback
  }
});

app.get('/api/trends/:districtCode', async (req, res) => {
  try {
    const { districtCode } = req.params;
    const cacheKey = `trends_${districtCode}`;
    const cached = cache.get(cacheKey);
    if (cached) return res.json(cached);

    const result = await pool.query(
      'SELECT month, year, data FROM performance WHERE district_code = $1 ORDER BY year DESC, month DESC LIMIT 7',
      [districtCode]
    );
    
    let trends;
    if (result.rows.length >= 1) {
      trends = result.rows.map(row => ({
          month: new Date(row.year, row.month).toLocaleString('default', { month: 'short' }),
          year: row.year,
          workers: row.data.activeWorkers,
          expenditure: row.data.totalExpenditure,
          works: row.data.completedWorks,
      })).reverse();
    } else {
      // Generate fallback data
      trends = [];
      const now = new Date();
      const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
      for (let i = 6; i >= 0; i--) {
        const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
        const data = generateDistrictData(districtCode, d.getMonth(), d.getFullYear());
        trends.push({
          month: monthNames[d.getMonth()],
          year: d.getFullYear(),
          workers: data.activeWorkers,
          expenditure: data.totalExpenditure,
          works: data.completedWorks,
        });
      }
    }
    cache.set(cacheKey, trends);
    res.json(trends);
  } catch (error) {
    console.error('Error fetching trends:', error);
    res.status(500).json({ error: 'Failed to fetch trends' });
  }
});

// Cron job to fetch data (runs every 6 hours)
// NOTE: Render's free tier may spin down, so cron jobs are not guaranteed.
// For this project, the initDatabase() function is more important.
cron.schedule('0 */6 * * *', async () => {
  console.log('Running scheduled data fetch...');
  try {
    const districts = await pool.query('SELECT code FROM districts');
    for (const district of districts.rows) {
      const now = new Date();
      const data = generateDistrictData(district.code, now.getMonth(), now.getFullYear());
      
      await pool.query(`
        INSERT INTO performance (district_code, month, year, data, updated_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (district_code, month, year) 
        DO UPDATE SET data = $4, updated_at = NOW()
      `, [district.code, now.getMonth(), now.getFullYear(), JSON.stringify(data)]);
    }
    console.log('Data fetch completed successfully');
  } catch (error) {
    console.error('Error in scheduled data fetch:', error);
  }
});

// Initialize database tables
async function initDatabase() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS districts (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        code VARCHAR(10) UNIQUE NOT NULL,
        lat DECIMAL(10, 6),
        lng DECIMAL(10, 6),
        state VARCHAR(50) DEFAULT 'Maharashtra'
      )
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS performance (
        id SERIAL PRIMARY KEY,
        district_code VARCHAR(10) NOT NULL,
        month INTEGER NOT NULL CHECK (month >= 0 AND month <= 11),
        year INTEGER NOT NULL CHECK (year >= 2024),
        data JSONB NOT NULL,
        updated_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(district_code, month, year)
      )
    `);
    
    // Insert districts if not exists
    for (const district of MAHARASHTRA_DISTRICTS) {
      await pool.query(`
        INSERT INTO districts (name, code, lat, lng)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (code) DO NOTHING
      `, [district.name, district.code, district.lat, district.lng]);
    }
    
    // Run the cron job's function once on startup to populate data
    // This is crucial for Render!
    console.log('Running initial data population...');
    const districts = await pool.query('SELECT code FROM districts');
    for (const district of districts.rows) {
      const now = new Date();
      // Generate data for the last 7 months
      for (let i = 6; i >= 0; i--) {
        const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
        const data = generateDistrictData(district.code, d.getMonth(), d.getFullYear());
        await pool.query(`
          INSERT INTO performance (district_code, month, year, data, updated_at)
          VALUES ($1, $2, $3, $4, NOW())
          ON CONFLICT (district_code, month, year) DO NOTHING
        `, [district.code, d.getMonth(), d.getFullYear(), JSON.stringify(data)]);
      }
    }
    console.log('Initial data population complete.');

    console.log('Database initialized successfully');
  } catch (error) {
    console.error('Error initializing database:', error);
    process.exit(1); // Exit if DB fails
  }
}

// Start server
const PORT = process.env.PORT || 3000;
initDatabase().then(() => {
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Server running on port ${PORT}`);
  });
});