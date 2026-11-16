-- Positions of each chunk in its original file (see versioned/000182).
ALTER TABLE chunks ADD COLUMN source_locators TEXT;
