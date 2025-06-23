const { Client } = require('pg');
const axios = require('axios');
const dbConfig = require('./db');


async function fetchApiData(courseId) {
  console.log("courseId",courseId)
  if (!courseId) return {};
  try {
    const response = await axios.post(
      'https://interface.prathamdigital.org/interface/v1/action/composite/v3/search',
      { request: { filters: { identifier: [courseId] } } },
      {
        headers: {
          'Content-Type': 'application/json',
          'Cookie': 'AWSALB=c6B+A+Xab3J969D5AKEyxg4pwghw+3S5jIuPlyoBPSY06OpzlB8Sx45e/MRPVHErjqfb23UDS2WalGwmwmtbJfjJBMaHrPQB47RoilfUz+x9VgL1hLQt6F51ZjGE; AWSALBCORS=c6B+A+Xab3J969D5AKEyxg4pwghw+3S5jIuPlyoBPSY06OpzlB8Sx45e/MRPVHErjqfb23UDS2WalGwmwmtbJfjJBMaHrPQB47RoilfUz+x9VgL1hLQt6F51ZjGE',
        },
      }
    );
      return response.data.result.QuestionSet[0]
    
  } catch (error) {
    console.error(`[API ERROR] Could not fetch data for courseId ${courseId}:`, error.message);
  }
  return {};
}


/**
 * Migrates the assessment_tracking_score_detail table with no modifications.
 * @param {Client} sourceClient - The client for the source database.
 * @param {Client} destClient - The client for the destination database.
 */
async function migrateScoreDetailsTable(sourceClient, destClient) {
  console.log('[MIGRATION] Starting migration for "assessment_tracking_score_detail" table...');
  
  const scoreDetailsResult = await sourceClient.query('SELECT * FROM public.assessment_tracking_score_detail');
  console.log(`[MIGRATION] Found ${scoreDetailsResult.rows.length} score detail records to migrate.`);

  for (const detail of scoreDetailsResult.rows) {
    // The ON CONFLICT clause failed because the destination table's 'id' column lacks a UNIQUE or PRIMARY KEY constraint.
    // To make the script re-runnable, we first DELETE any existing record with the same id, then INSERT the new one.
    await destClient.query('DELETE FROM public.assessment_tracking_score_detail WHERE id = $1', [detail.id]);
    
    const query = `
      INSERT INTO public.assessment_tracking_score_detail (
        id, "userId", "assessmentTrackingId", "questionId", pass, "sectionId", 
        "resValue", duration, score, "maxScore", "queTitle"
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);
    `;
    const values = [
      detail.id, detail.userId, detail.assessmentTrackingId, detail.questionId, detail.pass, detail.sectionId,
      detail.resValue, detail.duration, detail.score, detail.maxScore, detail.queTitle
    ];
    await destClient.query(query, values);
  }
  console.log('[MIGRATION] ✅ Finished migrating "assessment_tracking_score_detail" table.');
}

/**
 * Migrates the assessment_tracking table, enriching it with API data.
 * @param {Client} sourceClient - The client for the source database.
 * @param {Client} destClient - The client for the destination database.
 */
async function migrateAssessmentTrackingTable(sourceClient, destClient) {
  console.log('[MIGRATION] Starting migration for "assessment_tracking" table...');
    
  const assessmentResult = await sourceClient.query('SELECT * FROM public.assessment_tracking');
  console.log(`[MIGRATION] Found ${assessmentResult.rows.length} assessment records to migrate.`);

  for (const assessment of assessmentResult.rows) {
    const apiData = await fetchApiData(assessment.courseId);
    // console.log(assessment.courseId,apiData);
    
    const query = `
      INSERT INTO public.assessment_tracking (
        "assessmentTrackingId", "userId", "courseId", "contentId", "attemptId", "createdOn", 
        "lastAttemptedOn", "assessmentSummary", "totalMaxScore", "totalScore", "updatedOn", 
        "timeSpent", "unitId", name, description, subject, domain, "subDomain", channel, 
        "assessmentType", program, "targetAgeGroup", "assessmentName", "contentLanguage", 
        status, framework, "summaryType"
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27)
      ON CONFLICT ("assessmentTrackingId") DO UPDATE SET
        "userId" = EXCLUDED."userId", "courseId" = EXCLUDED."courseId", "contentId" = EXCLUDED."contentId",
        "attemptId" = EXCLUDED."attemptId", "lastAttemptedOn" = EXCLUDED."lastAttemptedOn", "assessmentSummary" = EXCLUDED."assessmentSummary",
        "totalMaxScore" = EXCLUDED."totalMaxScore", "totalScore" = EXCLUDED."totalScore", "updatedOn" = EXCLUDED."updatedOn",
        "timeSpent" = EXCLUDED."timeSpent", "unitId" = EXCLUDED."unitId", name = EXCLUDED.name, description = EXCLUDED.description,
        subject = EXCLUDED.subject, domain = EXCLUDED.domain, "subDomain" = EXCLUDED."subDomain", channel = EXCLUDED.channel,
        "assessmentType" = EXCLUDED."assessmentType", program = EXCLUDED.program, "targetAgeGroup" = EXCLUDED."targetAgeGroup",
        "assessmentName" = EXCLUDED."assessmentName", "contentLanguage" = EXCLUDED."contentLanguage", status = EXCLUDED.status,
        framework = EXCLUDED.framework, "summaryType" = EXCLUDED."summaryType";
    `;
    const assessmentSummaryString = assessment.assessmentSummary ? JSON.stringify(assessment.assessmentSummary) : null;

    // console.log("assessmentSummaryString",assessmentSummaryString)
    const values = [
      assessment.assessmentTrackingId, assessment.userId, assessment.courseId, assessment.contentId, assessment.attemptId,
      assessment.createdOn, assessment.lastAttemptedOn, assessmentSummaryString, assessment.totalMaxScore,
      assessment.totalScore, assessment.updatedOn, assessment.timeSpent, assessment.unitId,
      apiData?.name || null,
      apiData?.description || null,
      apiData?.subject?.[0] || null,
      apiData?.domain || null,
      apiData?.subDomain?.[0] || null,
      apiData?.channel || null,
      apiData?.assessmentType || null,
      apiData?.program?.[0] || null,
      apiData?.targetAgeGroup?.[0] || null,
      apiData?.name || null,
      apiData?.language?.[0] || null,
      apiData?.status || null,
      apiData?.framework || null,
      'assessment_tracking'
    ];
    console.log(values);

    await destClient.query(query, values);
    console.log(`[MIGRATION] Processed assessment record: ${assessment.assessmentTrackingId}`);
  }
  console.log('[MIGRATION] ✅ Finished migrating "assessment_tracking" table.');
}

/**
 * Main function to run the complete assessment data migration process.
 */
async function migrateAssessments() {
  console.log('=== STARTING ASSESSMENT MIGRATION ===');
  const sourceClient = new Client(dbConfig.assessment_source);
  const destClient = new Client(dbConfig.assessment_destination);

  try {
    // 1. Connect to both databases
    await sourceClient.connect();
    console.log('[MIGRATION] Connected to source database.');
    await destClient.connect();
    console.log('[MIGRATION] Connected to destination database.');

    // Step 1: Migrate score details table first (direct copy)
    // await migrateScoreDetailsTable(sourceClient, destClient);
    
    // Step 2: Then migrate the main assessment tracking table (with modifications)
    await migrateAssessmentTrackingTable(sourceClient, destClient);

    console.log('[MIGRATION] All tasks complete. Migration finished successfully.');
  } catch (error) {
    console.error('[MIGRATION] A critical error occurred:', error);
  } finally {
    // 4. Disconnect from both databases
    await sourceClient.end();
    await destClient.end();
    console.log('[MIGRATION] Disconnected from databases.');
  }
}

// Execute the migration if the script is run directly
if (require.main === module) {
  migrateAssessments().catch(err => {
    console.error('Migration failed with an unhandled error:', err);
    process.exit(1);
  });
}

module.exports = { migrateAssessments };