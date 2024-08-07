require('dotenv').config();
const fs = require('fs');
const path = require('path');
const Papa = require('papaparse');
const mysql = require('mysql2/promise');

// Create MySQL connection pool
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: parseInt(process.env.DB_PORT, 10),
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// Correctly formatted path to the local CSV file in the same folder
const csvFilePath = path.join(__dirname, 'FinalOutput.csv');

// Function to process CSV file and insert/update data into the database
const processCSV = async (filePath) => {
  try {
    const data = fs.readFileSync(filePath, 'utf8');

    // Parse CSV file
    const parsedData = Papa.parse(data, {
      header: true,
      skipEmptyLines: true,
    });

    // Log parsed data to check if CSV is read correctly
    console.log('CSV Data:', parsedData.data);
    console.log('CSV Headers:', parsedData.meta.fields);

    // Start a connection from the pool
    const connection = await pool.getConnection();

    // Begin transaction
    await connection.beginTransaction();

    let existingCount = 0; // Initialize count for existing emails
    let newCount = 0; // Initialize count for new emails
    let totalCount = 0; // Initialize total count of emails
    let changedRowsTotal = 0; // Initialize total count of changed rows

    try {
      for (let row of parsedData.data) {
        totalCount++; // Increment total email count
        const email = row['Reply-To Email'] ? row['Reply-To Email'] : row['From Email'];
        const name = row['Name'];
        const phone = row['Mobile Number'];

        // Check if email exists
        const [rows] = await connection.query('SELECT COUNT(*) as count FROM massemail WHERE email = ?', [email]);

        let result;

        if (rows[0].count > 0) {
          // Update record if email exists
          [result] = await connection.query('UPDATE massemail SET name = ?, phone = ? WHERE email = ?', [name, phone, email]);
          existingCount++; // Increment existing email count
          changedRowsTotal += result.changedRows; // Increment total changed rows count
          console.log(`Email ${totalCount} (Exist):`, result);
        } else {
          // Insert new record if email does not exist
          [result] = await connection.query('INSERT INTO massemail (email, name, phone) VALUES (?, ?, ?)', [email, name, phone]);
          newCount++; // Increment new email count
          changedRowsTotal += result.affectedRows; // Increment total changed rows count
          console.log(`Email ${totalCount} (Not Exist):`, result);
        }
      }

      // Commit transaction
      await connection.commit();
      console.log('Data successfully inserted/updated');
      console.log('Existing emails updated:', existingCount);
      console.log('New emails inserted:', newCount);
      console.log('Total number of emails processed:', totalCount);
      console.log('Total changed rows:', changedRowsTotal);
    } catch (err) {
      console.error('Failed to insert/update data:', err);
      await connection.rollback();
    } finally {
      // Release connection back to the pool
      connection.release();
    }
  } catch (err) {
    console.error('Failed to process CSV:', err);
  }
};

// Call the function to process the CSV file
processCSV(csvFilePath);

// Close connection pool after all operations are done
process.on('exit', () => {
  pool.end();
});
