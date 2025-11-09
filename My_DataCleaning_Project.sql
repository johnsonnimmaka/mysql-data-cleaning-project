-- ======================================================================
-- 🌍 MySQL Data Cleaning Project: Global Layoffs Dataset
-- Author: Johnson Nimmaka
-- Description: A complete data cleaning workflow in MySQL
-- ======================================================================

-- ----------------------------------------------------------------------
-- 1️⃣ CREATE DATABASE AND IMPORT RAW DATA
-- ----------------------------------------------------------------------

CREATE DATABASE world_layoffs;
USE world_layoffs;

-- Imported layoffs.csv as 'layoffs' table
SELECT * FROM layoffs;

-- Backup original table for safety
CREATE TABLE layoffs_staging LIKE layoffs;
INSERT INTO layoffs_staging SELECT * FROM layoffs;

-- From here onward, all operations will be done on 'layoffs_staging'
SELECT * FROM layoffs_staging;

-- ----------------------------------------------------------------------
-- 2️⃣ REMOVE DUPLICATES
-- ----------------------------------------------------------------------

-- Identify duplicate rows
SELECT company, location, industry, total_laid_off, percentage_laid_off,
       `date`, stage, country, funds_raised_millions,
       ROW_NUMBER() OVER (
         PARTITION BY company, location, industry, total_laid_off, 
                      percentage_laid_off, `date`, stage, country, funds_raised_millions
       ) AS row_num
FROM layoffs_staging;

-- Create a working table with an additional 'row_num' column
CREATE TABLE layoffs_staging2 LIKE layoffs_staging;
ALTER TABLE layoffs_staging2 ADD COLUMN row_num INT;

INSERT INTO layoffs_staging2
SELECT *, 
       ROW_NUMBER() OVER (
         PARTITION BY company, location, industry, total_laid_off, 
                      percentage_laid_off, `date`, stage, country, funds_raised_millions
       ) AS row_num
FROM layoffs_staging;

-- Delete duplicates (rows where row_num > 1)
DELETE FROM layoffs_staging2 WHERE row_num > 1;

-- ----------------------------------------------------------------------
-- 3️⃣ STANDARDIZE AND FIX DATA INCONSISTENCIES
-- ----------------------------------------------------------------------

-- Trim unwanted spaces in 'company' column
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Review distinct industry values
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- Standardize similar industry names (e.g., 'Crypto', 'CryptoCurrency', etc.)
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Standardize 'country' field (remove trailing dots)
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Convert 'date' from TEXT to proper DATE format
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- ----------------------------------------------------------------------
-- 4️⃣ HANDLE NULL AND BLANK VALUES
-- ----------------------------------------------------------------------

-- Convert blank industries to NULL
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Populate missing industries from matching company records
UPDATE layoffs_staging2 s1
JOIN layoffs_staging2 s2
  ON s1.company = s2.company
 AND s1.location = s2.location
SET s1.industry = s2.industry
WHERE s1.industry IS NULL
  AND s2.industry IS NOT NULL;

-- Remove rows with both 'total_laid_off' and 'percentage_laid_off' as NULL
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL 
  AND percentage_laid_off IS NULL;

-- ----------------------------------------------------------------------
-- 5️⃣ DROP UNNECESSARY COLUMNS
-- ----------------------------------------------------------------------

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- ----------------------------------------------------------------------
-- ✅ FINAL CLEANED DATA READY FOR ANALYSIS
-- ----------------------------------------------------------------------

SELECT * FROM layoffs_staging2;

-- 🎯 Data is now fully cleaned and ready for EDA or visualization tools.
-- ======================================================================
