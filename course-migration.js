const { Client } = require('pg');
const axios = require('axios');
const dbConfig = require('./db');

/**
 * Fetches course details from the middleware service API.
 * @param {string} courseId - The course identifier.
 * @returns {object} - The course content data from the API response.
 */
async function fetchCourseData(courseId) {
  if (!courseId) return {};
  try {
    const url = `${process.env.MIDDLEWARE_SERVICE_BASE_URL}api/course/v1/hierarchy/${courseId}?mode=edit`;
    console.log('Fetching course data from URL:', url);
    
    const headers = {
      'Content-Type': 'application/json',
    };

    const response = await axios.get(url, { headers });
    if (response.data.result && response.data.result.content) {
      return response.data.result.content;
    }
  } catch (error) {
    console.error(`[API ERROR] Could not fetch course data for courseId ${courseId}:`, error.message);
  }
  return {};
}

/**
 * Maps content data to course entity format.
 * @param {object} content - The content object from the API.
 * @returns {object} - The transformed course data.
 */
function mapContentToCourseEntity(content) {
  return {
    courseDoId: content.identifier,
    courseName: content.name,
    channel: content.channel,
    language: content.language || [],
    program: content.program || [],
    primaryUser: content.primaryUser || [],
    targetAgeGroup: content.targetAgeGroup || [],
    keywords: content.keywords || [],
    details: content, // save full original JSON here
  };
}

/**
 * Migrates the user_course_certificate table with no modifications.
 * @param {Client} sourceClient - The client for the source database.
 * @param {Client} destClient - The client for the destination database.
 */
async function migrateUserCourseDataTable(sourceClient, destClient) {
  console.log('[MIGRATION] Starting migration for "user_course_certificate" table...');
  
  const userCourseDataResult = await sourceClient.query('SELECT * FROM public.user_course_certificate');
  console.log(`[MIGRATION] Found ${userCourseDataResult.rows.length} user course data records to migrate.`);

  for (const userCourseData of userCourseDataResult.rows) {
    // Delete existing record first, then insert new one (same approach as assessment)
    await destClient.query('DELETE FROM public.user_course_certificate WHERE "usercertificateId" = $1', [userCourseData.usercertificateId]);
    
    const query = `
      INSERT INTO public.user_course_certificate (
        "usercertificateId", "userId", "courseId", "certificateId", "tenantId", status, 
        "issuedOn", "createdOn", "updatedOn", "completedOn", "completionPercentage", 
        progress, "lastReadContentId", "lastReadContentStatus", "createdBy"
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15);
    `;
    const values = [
      userCourseData.usercertificateId, userCourseData.userId, userCourseData.courseId, 
      userCourseData.certificateId, userCourseData.tenantId, userCourseData.status,
      userCourseData.issuedOn, userCourseData.createdOn, userCourseData.updatedOn,
      userCourseData.completedOn, userCourseData.completionPercentage, userCourseData.progress,
      userCourseData.lastReadContentId, userCourseData.lastReadContentStatus, userCourseData.createdBy
    ];
    await destClient.query(query, values);
  }
  console.log('[MIGRATION] ✅ Finished migrating "user_course_certificate" table.');
}

/**
 * Migrates the course table, enriching it with API data.
 * @param {Client} sourceClient - The client for the source database.
 * @param {Client} destClient - The client for the destination database.
 */
async function migrateCourseTable(sourceClient, destClient) {
  console.log('[MIGRATION] Starting migration for "course" table...');
    
  // Get unique course IDs from user_course_certificate to avoid duplicates
  const courseResult = await sourceClient.query('SELECT DISTINCT "courseId" FROM public.user_course_certificate');
  console.log(`[MIGRATION] Found ${courseResult.rows.length} unique courses to migrate.`);

  for (const courseRow of courseResult.rows) {
    const courseId = courseRow.courseId;
    const apiData = await fetchCourseData(courseId);
    const transformedData = mapContentToCourseEntity(apiData);

    // --- Data Sanitization for JSON columns ---
    const languageString = transformedData.language ? JSON.stringify(transformedData.language) : null;
    const programString = transformedData.program ? JSON.stringify(transformedData.program) : null;
    const primaryUserString = transformedData.primaryUser ? JSON.stringify(transformedData.primaryUser) : null;
    const targetAgeGroupString = transformedData.targetAgeGroup ? JSON.stringify(transformedData.targetAgeGroup) : null;
    const keywordsString = transformedData.keywords ? JSON.stringify(transformedData.keywords) : null;
    const detailsString = transformedData.details ? JSON.stringify(transformedData.details) : null;
    
    // Delete existing record first, then insert new one (same approach as assessment)
    await destClient.query('DELETE FROM public.course WHERE course_do_id = $1', [transformedData.courseDoId]);
    
    const query = `
      INSERT INTO public.course (
        course_do_id, course_name, channel, "language", "program", 
        primary_user, target_age_group, keywords, details
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
    `;
    
    const values = [
      transformedData.courseDoId,
      transformedData.courseName,
      transformedData.channel,
      languageString,
      programString,
      primaryUserString,
      targetAgeGroupString,
      keywordsString,
      detailsString
    ];

    await destClient.query(query, values);
    console.log(`[MIGRATION] Processed course: ${courseId}`);
  }
  console.log('[MIGRATION] ✅ Finished migrating "course" table.');
}

/**
 * Main function to run the complete course data migration process.
 */
async function migrateCourses() {
  console.log('=== STARTING COURSE MIGRATION ===');
  const sourceClient = new Client(dbConfig.assessment_source); // Using same source as assessment
  const destClient = new Client(dbConfig.assessment_destination); // Using same destination as assessment

  try {
    await sourceClient.connect();
    console.log('[MIGRATION] Connected to source database.');
    await destClient.connect();
    console.log('[MIGRATION] Connected to destination database.');

    // Step 1: Migrate user course data table (direct copy)
    await migrateUserCourseDataTable(sourceClient, destClient);
    
    // Step 2: Migrate course table (with API enrichment)
    await migrateCourseTable(sourceClient, destClient);

    console.log('[MIGRATION] All tasks complete. Course migration finished successfully.');
  } catch (error) {
    console.error('[MIGRATION] A critical error occurred:', error);
  } finally {
    await sourceClient.end();
    await destClient.end();
    console.log('[MIGRATION] Disconnected from databases.');
  }
}

// Execute the migration if the script is run directly
if (require.main === module) {
  migrateCourses().catch(err => {
    console.error('Course migration failed with an unhandled error:', err);
    process.exit(1);
  });
}

module.exports = { migrateCourses }; 