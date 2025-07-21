
{{ config(materialized='table') }}

SELECT
    CAST(number AS STRING) AS npi,
    CAST(enumeration_type AS STRING) AS enumeration_type,
    CAST(basic.status AS STRING) AS provider_status,
    CAST(basic.organization_name AS STRING) AS organization_name,
    CAST(basic.credential AS STRING) AS credential,
    CAST(basic.first_name AS STRING) AS first_name,
    CAST(basic.last_name AS STRING) AS last_name,
    
    -- Unnested Address Information
    address.address_purpose,
    address.address_type,
    address.address_1,
    address.address_2,
    address.city,
    address.state,
    address.postal_code,
    address.telephone_number,
    address.fax_number,

    -- Unnested Taxonomy Information
    taxonomy.code AS taxonomy_code,
    taxonomy.desc AS taxonomy_description,
    CAST(taxonomy.primary AS BOOLEAN) AS is_primary_taxonomy,
    taxonomy.license AS taxonomy_license,
    taxonomy.state AS taxonomy_state,

    -- Unnested Identifier Information
    identifier.identifier,
    identifier.issuer AS identifier_issuer,
    identifier.state AS identifier_state,
    identifier.desc AS identifier_description,

    -- Timestamps
    TIMESTAMP_MILLIS(CAST(created_epoch AS INT64)) AS created_at,
    TIMESTAMP_MILLIS(CAST(last_updated_epoch AS INT64)) AS last_updated_at,
    DATE(CAST(basic.enumeration_date AS TIMESTAMP)) AS enumeration_date

FROM
    {{ source('healthcare', 'npi') }}
LEFT JOIN UNNEST(addresses) AS address
LEFT JOIN UNNEST(taxonomies) AS taxonomy
LEFT JOIN UNNEST(identifiers) AS identifier