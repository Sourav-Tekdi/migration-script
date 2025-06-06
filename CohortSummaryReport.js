const { Client } = require('pg');
const { v4: uuidv4 } = require('uuid');
const dbConfig = require('./db');

console.log('=== Loading CohortSummaryReport.js ===');

// Import only specific functions to minimize initialization code
// Destructure only what we need to avoid running any initialization code
const UserProfileReport = require('./UserProfileReport');
const getCustomFields = UserProfileReport.getCustomFields;
const safeQuery = UserProfileReport.safeQuery;
const extractIdFromValue = UserProfileReport.extractIdFromValue;

console.log('=== Successfully imported functions from UserProfileReport ===');

async function migrateCohortSummaryReports() {
  console.log('=== STARTING COHORT SUMMARY REPORT MIGRATION ===');
  const sourceClient = new Client(dbConfig.source);
  const destClient = new Client(dbConfig.destination);

  try {
    await connectToDBs(sourceClient, destClient);

    await createCohortSummaryReportTable(destClient);
    
    // Query to get cohort data with member count and academic year info
    // Fixed query: Added quotes around ay.name -> ay."name"
    const cohortsQuery = `
  SELECT 
    c."cohortId", 
    c."name",
    c."type",
    c."tenantId",
    t."name" AS "tenantName",
    cay."academicYearId" AS "academicYear",
    COUNT(DISTINCT cm."userId") AS "memberCount",
    c."createdAt",
    c."updatedAt"
  FROM public."Cohort" c
  LEFT JOIN public."CohortMembers" cm ON c."cohortId" = cm."cohortId"
  LEFT JOIN public."CohortAcademicYear" cay ON c."cohortId" = cay."cohortId"
  LEFT JOIN public."Tenants" t ON c."tenantId" = t."tenantId"
  GROUP BY c."cohortId", c."name", c."type", c."tenantId", t."name", c."createdAt", c."updatedAt",cay."academicYearId"
`;


    const cohortsResult = await sourceClient.query(cohortsQuery);
    console.log(`[COHORT REPORT] Found ${cohortsResult.rows.length} cohorts to migrate`);

    for (const cohort of cohortsResult.rows) {
      await processCohort(sourceClient, destClient, cohort);
    }

    console.log('[COHORT REPORT] Cohort migration completed successfully');
  } catch (error) {
    console.error('[COHORT REPORT] Error during cohort migration:', error);
  } finally {
    await closeDBs(sourceClient, destClient);
  }
  console.log('=== COMPLETED COHORT SUMMARY REPORT MIGRATION ===');
}

async function connectToDBs(sourceClient, destClient) {
  await sourceClient.connect();
  console.log('[COHORT REPORT] Connected to source database');
  
  await destClient.connect();
  console.log('[COHORT REPORT] Connected to destination database');
}

async function closeDBs(sourceClient, destClient) {
  await sourceClient.end();
  console.log('[COHORT REPORT] Disconnected from source database');
  
  await destClient.end();
  console.log('[COHORT REPORT] Disconnected from destination database');
}

async function createCohortSummaryReportTable(client) {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS public."CohortSummaryReport" (
      "cohortId" UUID PRIMARY KEY,
      "name" VARCHAR,
      "type" VARCHAR,
      "tenantId" UUID,
      "tenantName" VARCHAR,
      "academicYear" VARCHAR,
      "memberCount" INTEGER,
      "customFields" JSONB,
      "createdAt" TIMESTAMP,
      "updatedAt" TIMESTAMP,
      "state" VARCHAR,
      "district" VARCHAR,
      "block" VARCHAR,
      "village" VARCHAR
    )
  `;
  
  await client.query(createTableQuery);
  console.log('[COHORT REPORT] CohortSummaryReport table created or already exists');
}

async function getCohortLocationIds(client, cohortId) {
  // Hardcoded field IDs for location fields
  const LOCATION_FIELD_IDS = {
    STATE: '6469c3ac-8c46-49d7-852a-00f9589737c5',
    DISTRICT: 'b61edfc6-3787-4079-86d3-37262bf23a9e',
    BLOCK: '4aab68ae-8382-43aa-a45a-e9b239319857',
    VILLAGE: '8e9bb321-ff99-4e2e-9269-61e863dd0c54'
  };
  
  // Query to get values for specific field IDs
  const query = `
    SELECT "fieldId", value
    FROM public."FieldValues"
    WHERE "itemId" = $1 
    AND "fieldId" = ANY($2::uuid[])
  `;
  
  const fieldIds = Object.values(LOCATION_FIELD_IDS);
  const rows = await safeQuery(client, query, [cohortId, fieldIds], [], 
    `Error fetching location field values for cohort ${cohortId}`);
  
  // Initialize with null values
  const locations = {
    stateId: null,
    districtId: null,
    blockId: null,
    villageId: null
  };
  
  // Log raw values for debugging
  console.log("Raw cohort location field values:", rows.map(r => ({ fieldId: r.fieldId, value: r.value })));
  
  // Just assign values directly without parsing
  rows.forEach(row => {
    // Map fieldId to the corresponding location ID
    if (row.fieldId === LOCATION_FIELD_IDS.STATE) {
      locations.stateId = row.value;
    } else if (row.fieldId === LOCATION_FIELD_IDS.DISTRICT) {
      locations.districtId = row.value;
    } else if (row.fieldId === LOCATION_FIELD_IDS.BLOCK) {
      locations.blockId = row.value;
    } else if (row.fieldId === LOCATION_FIELD_IDS.VILLAGE) {
      locations.villageId = row.value;
    }
  });
  
  console.log("Extracted cohort location IDs:", locations);
  return locations;
}

// Generic function to fetch location name by id and table
async function getLocationName(client, id, tableName, idColumn, nameColumn) {
  if (!id) return null;
  
  // Extract the ID from whatever format it's in
  const idValue = extractIdFromValue(id);
  if (!idValue) return null;
  
  console.log(`Looking up ${tableName} with ID: ${idValue}`); // Debug log
  
  const query = `SELECT ${nameColumn} FROM public."${tableName}" WHERE "${idColumn}" = $1`;
  
  try {
    const result = await client.query(query, [idValue]);
    console.log(result.rows, "Location Names");
    const name = result.rows.length > 0 ? result.rows[0][nameColumn] : null;
    console.log(`Found ${tableName} name: ${name}`); // Debug log
    return name;
  } catch (e) {
    console.warn(`Error querying ${tableName}: ${e.message}`);
    console.log(`Query was: ${query} with params [${idValue}]`); // Debug log
    return null;
  }
}

async function getLocationNames(client, locationIds) {
  const { stateId, districtId, blockId, villageId } = locationIds;
  
  // Define location table mappings
  const locationMappings = [
    { id: stateId, table: "state", idCol: "state_id", nameCol: "state_name", resultKey: "stateName" },
    { id: districtId, table: "district", idCol: "district_id", nameCol: "district_name", resultKey: "districtName" },
    { id: blockId, table: "block", idCol: "block_id", nameCol: "block_name", resultKey: "blockName" },
    { id: villageId, table: "village", idCol: "village_id", nameCol: "village_name", resultKey: "villageName" }
  ];
  
  // Initialize result object
  const result = {
    stateName: null,
    districtName: null,
    blockName: null,
    villageName: null
  };
  
  // Process each location in parallel for efficiency
  const locationPromises = locationMappings.map(async mapping => {
    const name = await getLocationName(client, mapping.id, mapping.table, mapping.idCol, mapping.nameCol);
    result[mapping.resultKey] = name;
  });
  
  await Promise.all(locationPromises);
  return result;
}

async function processCohort(sourceClient, destClient, cohort) {
  try {
    console.log(`[COHORT REPORT] Processing cohort: ${cohort.name} (${cohort.cohortId})`);
    
    // Get custom fields for cohort using the reusable function
    const customFields = await getCustomFields(sourceClient, cohort.cohortId);
    
    // Get location IDs for the cohort
    const locationIds = await getCohortLocationIds(sourceClient, cohort.cohortId);
    
    // Get location names from the IDs
    const locationNames = await getLocationNames(sourceClient, locationIds);
    
    // Insert or update the cohort summary report
    const insertQuery = `
      INSERT INTO public."CohortSummaryReport" (
        "cohortId", "name", "type", "tenantId", "tenantName", 
        "academicYear", "memberCount", "customFields", "createdAt", "updatedAt",
        "state", "district", "block", "village"
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
      ON CONFLICT ("cohortId") 
      DO UPDATE SET 
        "name" = EXCLUDED."name",
        "type" = EXCLUDED."type",
        "tenantId" = EXCLUDED."tenantId",
        "tenantName" = EXCLUDED."tenantName",
        "academicYear" = EXCLUDED."academicYear",
        "memberCount" = EXCLUDED."memberCount",
        "customFields" = EXCLUDED."customFields",
        "updatedAt" = EXCLUDED."updatedAt",
        "state" = EXCLUDED."state",
        "district" = EXCLUDED."district",
        "block" = EXCLUDED."block",
        "village" = EXCLUDED."village"
      RETURNING "cohortId", "name"
    `;

    const result = await destClient.query(insertQuery, [
      cohort.cohortId,
      cohort.name,
      cohort.type,
      cohort.tenantId,
      cohort.tenantName,
      cohort.academicYear,
      cohort.memberCount,
      JSON.stringify(customFields),
      cohort.createdAt,
      cohort.updatedAt,
      locationNames.stateName,
      locationNames.districtName,
      locationNames.blockName,
      locationNames.villageName
    ]);
    
    if (result.rows && result.rows.length > 0) {
      console.log(`[COHORT REPORT] ✅ Successfully inserted/updated cohort: ${result.rows[0].name}`);
    } else {
      console.log(`[COHORT REPORT] ⚠️ No confirmation data returned for cohort: ${cohort.name}`);
    }
    
    console.log(`[COHORT REPORT] Completed processing cohort: ${cohort.name} (${cohort.cohortId})`);
  } catch (error) {
    console.error(`[COHORT REPORT] Error processing cohort ${cohort.cohortId}:`, error);
  }
}

// Run the migration only if this script is run directly (not imported)
if (require.main === module) {
  console.log('Running CohortSummaryReport.js directly');
  migrateCohortSummaryReports().catch(err => {
    console.error('Cohort migration failed:', err);
    process.exit(1);
  });
} else {
  console.log('CohortSummaryReport.js loaded as a module');
}

module.exports = {
  migrateCohortSummaryReports
};