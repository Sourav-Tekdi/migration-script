const { Client } = require('pg');
const { v4: uuidv4 } = require('uuid');
const dbConfig = require('./db');

async function migrateUserProfileReports() {
  console.log('=== STARTING USER PROFILE REPORT MIGRATION ===');
  const sourceClient = new Client(dbConfig.source);
  const destClient = new Client(dbConfig.destination);

  try {
    await connectToDBs(sourceClient, destClient);

    await createUserProfileReportTable(destClient);
    const usersQuery = `
      SELECT 
        u."userId", 
        u.username,
        CONCAT(
          COALESCE(u."firstName", ''), ' ', 
          COALESCE(u."middleName", ''), ' ', 
          COALESCE(u."lastName", '')
        ) AS "fullName",
        u.email,
        u.mobile,
        u.dob,
        u.gender::text,
        u.status::text,
        u."createdAt",
        u."updatedAt",
        u."createdBy",
        u."updatedBy"
      FROM public."Users" u
    `;

    const usersResult = await sourceClient.query(usersQuery);
    console.log(`[USER REPORT] Found ${usersResult.rows.length} users to migrate`);

    for (const user of usersResult.rows) {
      await processUser(sourceClient, destClient, user);
    }

    console.log('[USER REPORT] Migration completed successfully');
  } catch (error) {
    console.error('[USER REPORT] Error during migration:', error);
  } finally {
    await closeDBs(sourceClient, destClient);
  }
  console.log('=== COMPLETED USER PROFILE REPORT MIGRATION ===');
}

async function connectToDBs(sourceClient, destClient) {
  await sourceClient.connect();
  console.log('[USER REPORT] Connected to source and destination databases');
  await destClient.connect();
}

async function closeDBs(sourceClient, destClient) {
  await sourceClient.end();
  await destClient.end();
  console.log('[USER REPORT] Database connections closed');
}

async function processUser(sourceClient, destClient, user) {
  console.log(`[USER REPORT] Processing user: ${user.username} (${user.userId})`);
  
  // Get custom fields
  const customFields = await getCustomFields(sourceClient, user.userId);
  
  // Get location IDs directly from dedicated function
  const locationIds = await getUserLocationIds(sourceClient, user.userId);
  
  // Get location names from respective tables
  const locationNames = await getLocationNames(sourceClient, locationIds);
  
  const cohorts = await getUserCohorts(sourceClient, user.userId);
  
  // Get user's tenant and role information
  const { tenantId, tenantName, roleName, roleId } = await getUserTenantAndRole(sourceClient, user.userId);
  
  // Determine if user is an automatic member based on cohort data
  const automaticMember = determineAutomaticMembership(cohorts);

  // Insert into destination table
  await insertOrUpdateUserProfileReport(destClient, {
    userId: user.userId,
    username: user.username,
    fullName: user.fullName.trim(),
    email: user.email || null,
    mobile: user.mobile || null,
    dob: user.dob || null,
    gender: user.gender || null,
    tenantId: tenantId,
    tenantName: tenantName,
    status: user.status,
    createdAt: user.createdAt,
    updatedAt: user.updatedAt,
    createdBy: user.createdBy,
    updatedBy: user.updatedBy,
    roleName: roleName,
    roleId: roleId,
    customFields: customFields,
    cohorts: cohorts,
    automaticMember: automaticMember ? 1 : 0,
    state: locationNames.stateName,
    district: locationNames.districtName,
    block: locationNames.blockName,
    village: locationNames.villageName
  });
  
  console.log(`[USER REPORT] Processed user: ${user.username}`);
}

async function createUserProfileReportTable(client) {
  const query = `
    CREATE TABLE IF NOT EXISTS "UserProfileReport" (
      "userId" uuid PRIMARY KEY,
      "username" varchar,
      "fullName" varchar,
      "email" varchar,
      "mobile" varchar,
      "dob" varchar,
      "gender" varchar,
      "tenantId" uuid,
      "tenantName" varchar,
      "status" varchar,
      "createdAt" timestamp,
      "updatedAt" timestamp,
      "createdBy" uuid,
      "updatedBy" uuid,
      "roleId" uuid,
      "roleName" varchar,
      "customFields" jsonb,
      "cohorts" jsonb,
      "automaticMember" boolean,
      "state" varchar,
      "district" varchar,
      "block" varchar,
      "village" varchar
    )
  `;
  await client.query(query);
  console.log('[USER REPORT] UserProfileReport table created if not exists');
}

// Generic function to safely execute database queries
async function safeQuery(client, query, params, defaultValue, errorMessage) {
  try {
    const result = await client.query(query, params);
    return result.rows;
  } catch (error) {
    console.warn(`${errorMessage}: ${error.message}`);
    return defaultValue;
  }
}

async function getCustomFields(client, userId) {
  const query = `
    SELECT fv."fieldId", f.name, f.type, fv.value
    FROM public."FieldValues" fv
    JOIN public."Fields" f ON fv."fieldId" = f."fieldId"
    WHERE fv."itemId" = $1
  `;
  
  const rows = await safeQuery(client, query, [userId], [], 
    `Error fetching custom fields for user ${userId}`);
  
  const customFields = {};
  rows.forEach(row => {
    customFields[row.name] = {
      type: row.type,
      value: row.value
    };
  });
  
  return customFields;
}

async function getUserLocationIds(client, userId) {
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
  const rows = await safeQuery(client, query, [userId, fieldIds], [], 
    `Error fetching location field values for user ${userId}`);
  
  // Initialize with null values
  const locations = {
    stateId: null,
    districtId: null,
    blockId: null,
    villageId: null
  };
  
  // Log raw values for debugging
  console.log("[USER REPORT] Raw location field values:", rows.map(r => ({ fieldId: r.fieldId, value: r.value })));
  
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
  
  console.log("[USER REPORT] Extracted location IDs:", locations);
  return locations;
}

// Helper function to extract integer from array or simple value
function extractIdFromValue(value) {
  if (!value) return null;
  
  // If it's a number, return it directly
  if (typeof value === 'number') return value;
  
  // If it's an array, get the first element
  if (Array.isArray(value) && value.length > 0) {
    const firstValue = value[0];
    // If the first element is a number or numeric string, return as integer
    if (typeof firstValue === 'number') return firstValue;
    if (typeof firstValue === 'string' && !isNaN(firstValue)) {
      return parseInt(firstValue, 10);
    }
  }
  
  // If it's a string with a number (with or without braces)
  if (typeof value === 'string') {
    const matches = value.match(/\d+/);
    if (matches && matches.length > 0) {
      return parseInt(matches[0], 10);
    }
    // If it's a numeric string without any special characters
    if (!isNaN(value)) {
      return parseInt(value, 10);
    }
  }
  
  console.warn(`[USER REPORT] Could not extract ID from value: ${JSON.stringify(value)}`);
  return null;
}

// Generic function to fetch location name by id and table
async function getLocationName(client, id, tableName, idColumn, nameColumn) {
  if (!id) return null;
  
  // Extract the ID from whatever format it's in
  const idValue = extractIdFromValue(id);
  if (!idValue) return null;
  
  console.log(`[USER REPORT] Looking up ${tableName} with ID: ${idValue}`);
  
  const query = `SELECT ${nameColumn} FROM public."${tableName}" WHERE "${idColumn}" = $1`;
  
  try {
    const result = await client.query(query, [idValue]);
    console.log(result.rows, "[USER REPORT] Location Names");
    const name = result.rows.length > 0 ? result.rows[0][nameColumn] : null;
    console.log(`[USER REPORT] Found ${tableName} name: ${name}`);
    return name;
  } catch (e) {
    console.warn(`[USER REPORT] Error querying ${tableName}: ${e.message}`);
    console.log(`[USER REPORT] Query was: ${query} with params [${idValue}]`);
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

async function getUserCohorts(client, userId) {
  const query = `
    SELECT 
      c."cohortId",
      c.name AS "cohortName",
      c.type AS "cohortType",
      c."tenantId",
      c."status" AS "cohortStatus",
      cm.status as "cohortMemberStatus",
      cm."createdAt" AS "joinedAt"
    FROM public."CohortMembers" cm
    JOIN public."Cohort" c ON cm."cohortId" = c."cohortId"
    WHERE cm."userId" = $1
  `;
  
  const rows = await safeQuery(client, query, [userId], [], 
    `Error fetching cohorts for user ${userId}`);
  
  return rows.map(row => ({
    cohortId: row.cohortId,
    cohortName: row.cohortName,
    cohortType: row.cohortType,
    tenantId: row.tenantId,
    cohortMemberStatus: row.cohortMemberStatus,
    cohortStatus: row.cohortStatus,
    joinedAt: row.joinedAt
  }));
}

async function getUserTenantAndRole(client, userId) {
  const query = `
    SELECT 
      utm."tenantId",
      t.name AS "tenantName",
      urm."roleId",
      r.name AS "roleName",
      r.code AS "roleCode"
    FROM public."UserTenantMapping" utm
    LEFT JOIN public."Tenants" t ON utm."tenantId" = t."tenantId"
    LEFT JOIN public."UserRolesMapping" urm ON urm."userId" = utm."userId"
    LEFT JOIN public."Roles" r ON urm."roleId" = r."roleId"
    WHERE utm."userId" = $1
    LIMIT 1
  `;
  
  const rows = await safeQuery(client, query, [userId], [], 
    `Error fetching tenant and role for user ${userId}`);
  
  if (rows.length > 0) {
    const row = rows[0];
    return {
      tenantId: row.tenantId,
      tenantName: row.tenantName,
      roleId: row.roleId,
      roleName: row.roleName,
      roleCode: row.roleCode
    };
  }
  
  // If no results, return null values
  return {
    tenantId: null,
    tenantName: null,
    roleId: null,
    roleName: null,
    roleCode: null
  };
}

function determineAutomaticMembership(cohorts) {
  // Logic to determine if a user is an automatic member
  if (cohorts.length === 0) return false;
  
  // Example: user is automatic member if they belong to a cohort of type 'automatic'
  return cohorts.some(cohort => cohort.cohortType === 'automatic');
}

async function insertOrUpdateUserProfileReport(client, userData) {
  const query = `
    INSERT INTO "UserProfileReport" (
      "userId", "username", "fullName", "email", "mobile", "dob", "gender",
      "tenantId", "tenantName", "status", "createdAt", "updatedAt",
      "createdBy", "updatedBy", "roleId", "roleName", "customFields", "cohorts", "automaticMember",
      "state", "district", "block", "village"
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
    ON CONFLICT ("userId") 
    DO UPDATE SET
      "username" = $2,
      "fullName" = $3,
      "email" = $4,
      "mobile" = $5,
      "dob" = $6,
      "gender" = $7,
      "tenantId" = $8,
      "tenantName" = $9,
      "status" = $10,
      "updatedAt" = $12,
      "updatedBy" = $14,
      "roleId" = $15,
      "roleName" = $16,
      "customFields" = $17,
      "cohorts" = $18,
      "automaticMember" = $19,
      "state" = $20,
      "district" = $21,
      "block" = $22,
      "village" = $23
  `;
  
  const values = [
    userData.userId,
    userData.username,
    userData.fullName,
    userData.email,
    userData.mobile,
    userData.dob,
    userData.gender,
    userData.tenantId,
    userData.tenantName,
    userData.status,
    userData.createdAt,
    userData.updatedAt,
    userData.createdBy,
    userData.updatedBy,
    userData.roleId,
    userData.roleName,
    JSON.stringify(userData.customFields),
    JSON.stringify(userData.cohorts),
    userData.automaticMember,
    userData.state,
    userData.district,
    userData.block,
    userData.village
  ];
  
  await client.query(query, values);
}

// First export the utility functions that might be imported by other modules
// This ensures they're available for import before any initialization happens
exports.getCustomFields = getCustomFields;
exports.safeQuery = safeQuery;
exports.extractIdFromValue = extractIdFromValue;
exports.connectToDBs = connectToDBs;
exports.closeDBs = closeDBs;
exports.migrateUserProfileReports = migrateUserProfileReports;

// Execute the migration only if this script is run directly (not imported)
if (require.main === module) {
  console.log('Running UserProfileReport.js directly');
  migrateUserProfileReports().catch(err => {
    console.error('Migration failed:', err);
    process.exit(1);
  });
}