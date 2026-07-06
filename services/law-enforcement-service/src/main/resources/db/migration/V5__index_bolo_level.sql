-- The BOLO API filters on bolo_level (HIGH/CRITICAL) with a newest-first LIMIT. Almost all
-- rows are NONE, so a partial index keeps it tiny and makes the lookup an index scan.
CREATE INDEX idx_law_enforcement_accidents_bolo_level
    ON law_enforcement_accidents (bolo_level, id DESC)
    WHERE bolo_level <> 'NONE';
