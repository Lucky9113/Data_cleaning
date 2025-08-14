-- Exploratory Data Analysis

Select *
from layoffs_staging2;

Select MAX(total_laid_off) , max(percentage_laid_off)
from layoffs_staging2;

Select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc
;

select company, SUM(total_laid_off)
from layoffs_staging2
group by company
order by 2 DESC;


select min(`date`), MAX(`date`)
from layoffs_staging2;

select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

Select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 1 desc;

Select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 desc;

Select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

Select substring(`date`,1,7) as `MONTH`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by substring(`date`,1,7)
order by 1 desc;

with rolling_total as
(
Select substring(`date`,1,7) as `MONTH`, sum(total_laid_off) total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by substring(`date`,1,7)
order by 1 desc
)
select `MONTH`,total_off, sum(total_off) over(order by `MONTH`) as rolling_total
from rolling_total;

Select company, YEAR(`date`),sum(total_laid_off)
from layoffs_staging2
group by company,YEAR(`date`)
order by 3 desc;

with company_year (company, years, total_laid_off) as
(
Select company, YEAR(`date`),sum(total_laid_off)
from layoffs_staging2
group by company,YEAR(`date`)
), Company_Year_Rank as (
select *, dense_rank() over ( partition by years order by total_laid_off desc) as Ranking
from company_year
where years is not null
)
select *
from company_year_rank
where Ranking <=5;
;