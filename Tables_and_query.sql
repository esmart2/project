

CREATE TABLE raw_patient_record (
  id TEXT PRIMARY KEY,
  first_name              TEXT,
  last_name               TEXT,
  dob                     DATE,
  ssn_last4               CHAR(8),
  phone                   TEXT,
  address                 TEXT,
  facility_id             TEXT,
  medical_record_number   TEXT,
  import_date             TIMESTAMPTZ DEFAULT now()
);


create materialized view if not exists deduplicated_patients as
WITH simple_filter as (
  SELECT
    max(id) as id,
    max(first_name) as first_name,
    max(first_name_metaphone_1) as first_name_metaphone_1,
    max(first_name_initial) as first_name_initial,
    max(last_name) as last_name,
    max(last_name_metaphone_1) as last_name_metaphone_1,
    max(last_name_initial) as last_name_initial,
    max(dob) as dob,
    max(ssn_last4) as ssn_last4,
    max(phone) as phone,
    max(address) as address,
    facility_id,
    medical_record_number,
    max(import_date) as import_date
  FROM raw_patient_entity
  GROUP BY facility_id, medical_record_number
),
 blocked_data as (
  SELECT *,
    coalesce(
      CASE WHEN dob is not null and first_name_initial is not null and last_name_metaphone_1 is not null
        THEN 'DFILM1:' || dob || '|' || first_name_initial || '|' || last_name_metaphone_1 end,
      CASE WHEN address is not null and dob is not null
        THEN 'AD:' || address || '|' || dob end,
      'ID:' || id
    ) as entity_key_1,
    coalesce(
      case when last_name_metaphone_1 is not null and dob is not null and phone is not null
        THEN 'LM1DP:' || last_name_metaphone_1 || '|' || dob || '|' || phone end,
      CASE WHEN first_name_metaphone_1 is not null and last_name_initial is not null and dob is not null
        THEN 'FM1LID:' || first_name_metaphone_1 || '|' || last_name_initial || '|' || dob end,
      'LM1:' || last_name_metaphone_1
    ) as entity_key_2
  FROM simple_filter
),
first_grouping as (
  SELECT entity_key_1,
    MIN(id) as id,
    (ARRAY_REMOVE(ARRAY_AGG(entity_key_2),null))[1] as entity_key_2,
    (ARRAY_REMOVE(ARRAY_AGG(first_name),null))[1] as first_name,
    (ARRAY_REMOVE(ARRAY_AGG(last_name),null))[1] as last_name,
    (ARRAY_REMOVE(ARRAY_AGG(dob),null))[1] as dob,
    (ARRAY_REMOVE(ARRAY_AGG(ssn_last4),null))[1] as ssn_last4,
    (ARRAY_REMOVE(ARRAY_AGG(address),null))[1] as address,
    (ARRAY_REMOVE(ARRAY_AGG(phone),null))[1] as phone,
    coalesce(JSONB_AGG(DISTINCT NULLIF(facility_id, '')) filter (where phone is not null), '[]'::jsonb) AS facility_id,
    coalesce(JSONB_AGG(DISTINCT NULLIF(medical_record_number, '')) filter (where phone is not null), '[]'::jsonb) AS medical_record_number,
    (ARRAY_REMOVE(ARRAY_AGG(import_date),null))[1] as import_date
  FROM blocked_data
  GROUP BY entity_key_1
)
SELECT
    MIN(id) as id,
    (ARRAY_REMOVE(ARRAY_AGG(first_name),null))[1] as first_name,
    (ARRAY_REMOVE(ARRAY_AGG(last_name),null))[1] as last_name,
    (ARRAY_REMOVE(ARRAY_AGG(dob),null))[1] as dob,
    (ARRAY_REMOVE(ARRAY_AGG(ssn_last4),null))[1] as ssn_last4,
    (ARRAY_REMOVE(ARRAY_AGG(address),null))[1] as address,
    (ARRAY_REMOVE(ARRAY_AGG(phone),null))[1] as phone,
    (ARRAY_REMOVE(ARRAY_AGG(facility_id),null))[1] as facility_id,
    (ARRAY_REMOVE(ARRAY_AGG(medical_record_number),null))[1] as medical_record_number,
    (ARRAY_REMOVE(ARRAY_AGG(import_date),null))[1] as import_date
FROM first_grouping
GROUP BY entity_key_2
ORDER BY first_name, last_name;
