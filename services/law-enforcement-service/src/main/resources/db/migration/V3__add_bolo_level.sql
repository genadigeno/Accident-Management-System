-- BOLO ("Be On the Lookout") severity classified from the incident description.
ALTER TABLE law_enforcement_accidents
    ADD COLUMN bolo_level VARCHAR(20) NOT NULL DEFAULT 'NONE';
