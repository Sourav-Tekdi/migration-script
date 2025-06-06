const { Client } = require('pg');
const { v4: uuidv4 } = require('uuid');
const dbConfig = require('./db');

// Import only specific functions to minimize initialization code
const UserProfileReport = require('./UserProfileReport');
const safeQuery = UserProfileReport.safeQuery;

console.log('=== Loading AttendanceReport.js ===');

async function migrateAttendanceReports() {
  console.log('=== STARTING DAILY ATTENDANCE REPORT MIGRATION ===');
  const sourceClient = new Client(dbConfig.attendance_source);
  const destClient = new Client(dbConfig.attendance_destination);

  try {
    await connectToDBs(sourceClient, destClient);

    await createDailyAttendanceReportTable(destClient);
    
    // Query to get attendance data
    const attendanceQuery = `
      SELECT 
        a."attendanceId",
        a."userId",
        a."contextId",
        a.context,
        a."attendanceDate",
        a.attendance,
        a."metaData",
        a."createdAt",
        a."updatedAt",
        a."createdBy",
        a."updatedBy"
      FROM public."Attendance" a
    `;

    const attendanceResult = await sourceClient.query(attendanceQuery);
    console.log(`[ATTENDANCE REPORT] Found ${attendanceResult.rows.length} attendance records to migrate`);
    
    for (const attendance of attendanceResult.rows) {
      await processAttendance(sourceClient, destClient, attendance);
    }

    console.log('[ATTENDANCE REPORT] Attendance migration completed successfully');
  } catch (error) {
    console.error('[ATTENDANCE REPORT] Error during attendance migration:', error);
  } finally {
    await closeDBs(sourceClient, destClient);
  }
  console.log('=== COMPLETED DAILY ATTENDANCE REPORT MIGRATION ===');
}

async function connectToDBs(sourceClient, destClient) {
  await sourceClient.connect();
  console.log('[ATTENDANCE REPORT] Connected to source database');
  
  await destClient.connect();
  console.log('[ATTENDANCE REPORT] Connected to destination database');
}

async function closeDBs(sourceClient, destClient) {
  await sourceClient.end();
  console.log('[ATTENDANCE REPORT] Disconnected from source database');
  
  await destClient.end();
  console.log('[ATTENDANCE REPORT] Disconnected from destination database');
}

async function createDailyAttendanceReportTable(client) {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS public."DailyAttendanceReport" (
      "attendanceId" UUID PRIMARY KEY,
      "userId" UUID,
      "cohortId" UUID,
      "context" VARCHAR,
      "date" DATE,
      "status" VARCHAR,
      "metadata" VARCHAR,
      "createdAt" TIMESTAMP,
      "updatedAt" TIMESTAMP,
      "createdBy" UUID,
      "updatedBy" UUID
    )
  `;
  
  await client.query(createTableQuery);
  console.log('[ATTENDANCE REPORT] DailyAttendanceReport table created or already exists');
}

async function processAttendance(sourceClient, destClient, attendance) {
  try {
    console.log(`[ATTENDANCE REPORT] Processing attendance record: ${attendance.attendanceId}`);
    
    // Prepare metadata
    let metadata = attendance.metaData;
    
    // Insert or update the attendance record
    const insertQuery = `
      INSERT INTO public."DailyAttendanceReport" (
        "attendanceId", "userId", "cohortId", "context", "date", 
        "status", "metadata", "createdAt", "updatedAt", "createdBy", "updatedBy"
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
      ON CONFLICT ("attendanceId") 
      DO UPDATE SET 
        "userId" = EXCLUDED."userId",
        "cohortId" = EXCLUDED."cohortId",
        "context" = EXCLUDED."context",
        "date" = EXCLUDED."date",
        "status" = EXCLUDED."status",
        "metadata" = EXCLUDED."metadata",
        "updatedAt" = EXCLUDED."updatedAt",
        "updatedBy" = EXCLUDED."updatedBy"
      RETURNING "attendanceId"
    `;

    const result = await destClient.query(insertQuery, [
      attendance.attendanceId,
      attendance.userId,
      attendance.contextId,
      attendance.context,
      attendance.attendanceDate,
      attendance.attendance,
      metadata,
      attendance.createdAt,
      attendance.updatedAt,
      attendance.createdBy,
      attendance.updatedBy
    ]);
    
    if (result.rows && result.rows.length > 0) {
      console.log(`[ATTENDANCE REPORT] ✅ Successfully inserted/updated attendance record: ${result.rows[0].attendanceId}`);
    } else {
      console.log(`[ATTENDANCE REPORT] ⚠️ No confirmation data returned for attendance record: ${attendance.attendanceId}`);
    }
    
    console.log(`[ATTENDANCE REPORT] Completed processing attendance record: ${attendance.attendanceId}`);
  } catch (error) {
    console.error(`[ATTENDANCE REPORT] Error processing attendance record ${attendance.attendanceId}:`, error);
  }
}

// Run the migration only if this script is run directly (not imported)
if (require.main === module) {
  console.log('Running AttendanceReport.js directly');
  migrateAttendanceReports().catch(err => {
    console.error('Attendance migration failed:', err);
    process.exit(1);
  });
} else {
  console.log('AttendanceReport.js loaded as a module');
}

module.exports = {
  migrateAttendanceReports
};
