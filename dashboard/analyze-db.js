const { Client } = require('pg');

const client = new Client({
    connectionString: 'postgresql://postgres:KeKhbaTFLnFLpIoFAmchXmeoRPERJHNN@metro.proxy.rlwy.net:26111/railway',
    // Usually external Railway connections need SSL
    ssl: { rejectUnauthorized: false }
});

async function main() {
    try {
        await client.connect();
        console.log("Connected successfully.");

        console.log("--- Tables ---");
        const res = await client.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public'
    `);
        console.log(res.rows.map(r => r.table_name));

        const tableNameGuess = res.rows.find(r => r.table_name.toLowerCase() === 'therapist') || 'Therapist';
        // If found, use the actual name (tableNameGuess.table_name if object, or just guess)
        const actualTableName = typeof tableNameGuess === 'object' ? tableNameGuess.table_name : tableNameGuess;

        console.log(`\n--- Columns for ${actualTableName} ---`);
        const cols = await client.query(`
      SELECT column_name, data_type, is_nullable
      FROM information_schema.columns
      WHERE table_name = $1
    `, [actualTableName]);
        console.log(cols.rows);

        console.log(`\n--- Sample Data for ${actualTableName} ---`);
        const data = await client.query(`SELECT * FROM "${actualTableName}" LIMIT 5`);
        console.log(JSON.stringify(data.rows, null, 2));

    } catch (err) {
        console.error("Error:", err);
    } finally {
        await client.end();
    }
}

main();
