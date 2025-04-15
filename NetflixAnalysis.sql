use kaggle;


create table netflix_data (
    show_id varchar(20) primary key,
    type varchar(20),
    title nvarchar(255),
    director varchar(255),
    cast text,
    country varchar(255),
    date_added varchar(20),
    release_year int,
    rating varchar(10),
    duration varchar(20),
    listed_in text,
    description text
);

select * from kaggle.netflix_data nd limit 50;

select * from kaggle.netflix_data nd where nd.show_id = 's5023';



---- remove duplicates

select show_id, count(1) from kaggle.netflix_data nd 
group by show_id 
having count(1)>1;

select title , count(*)
from kaggle.netflix_data nd
group by title 
having count(*) > 1;

select * from kaggle.netflix_data nd 
where title like 'Veronica';

select title ,`type`,count(*) as count 
from kaggle.netflix_data nd 
group by title ,`type` 
having count(*)>1 ;



select * from kaggle.netflix_data nd 
where concat(title,type) in (
select concat(title,type)
from kaggle.netflix_data nd
group by title,type
having count(*) > 1
)
order by title 


create table netflix_staging as
with cte as (select *,
		row_number() over(partition by title, type order by show_id) as cnt
		from kaggle.netflix_data nd
			)
select show_id,
		`type`,
		title,str_to_date(date_added, '%M %d, %Y') as date_added,
		release_year,
		rating,
		case when duration is null then rating else duration end as duration,
		description	
from cte 
where cte.cnt = 1 --  and cte.date_added is null


select * from kaggle.netflix_staging ns ;
select * from kaggle.netflix_data nd;


select director, count(*)
from kaggle.netflix_data nd 
group by director
having count(*) > 1
;

--    	CREATE NEW TABLES FOR director,listed_in,country,cast TO SEPERATE OUT THE MULTIPLE VALUES 

-- MS SQL Logic
-- select director, trim(value) as director 
-- from kaggle.netflix_data nd
-- cross apply string_split(director,',')


-- create table netflix_directors as
with recursive split_cte as (
  select
    show_id,
    substring_index(director, ',', 1) as director,  --  Gets the first name before the comma  
    substring(director, length(substring_index(director, ',', 1)) + 2) as remaining  -- Gets the rest of the string after the first comma + space
  from kaggle.netflix_data
  where director is not null

  union all

  select
    show_id,
    substring_index(remaining, ',', 1),
    substring(remaining, length(substring_index(remaining, ',', 1)) + 2)
  from split_cte
  where remaining != ''
)
select show_id, trim(director) as director
from split_cte
-- where show_id = 's102';


create table netflix_genre as 
with recursive split_cte as (
  select
    show_id,
    substring_index(listed_in, ',', 1) as genre,  --  Gets the first name before the comma  
    substring(listed_in, length(substring_index(listed_in, ',', 1)) + 2) as remaining  -- Gets the rest of the string after the first comma + space
  from kaggle.netflix_data
  where listed_in is not null

  union all

  select
    show_id,
    substring_index(remaining, ',', 1),
    substring(remaining, length(substring_index(remaining, ',', 1)) + 2)
  from split_cte
  where remaining != ''
)
select show_id, trim(genre) as genre
from split_cte;




create table netflix_country as 
with recursive split_cte as (
  select
    show_id,
    substring_index(country, ',', 1) as country,  --  Gets the first name before the comma  
    substring(country, length(substring_index(country, ',', 1)) + 2) as remaining  -- Gets the rest of the string after the first comma + space
  from kaggle.netflix_data
  where country is not null

  union all

  select
    show_id,
    substring_index(remaining, ',', 1),
    substring(remaining, length(substring_index(remaining, ',', 1)) + 2)
  from split_cte
  where remaining != ''
)
select show_id, trim(country) as country
from split_cte
-- where show_id = 's100';







create table netflix_cast as 
with recursive split_cte as (
  select
    show_id,
    substring_index(cast, ',', 1) as cast,  --  Gets the first name before the comma  
    substring(cast, length(substring_index(cast, ',', 1)) + 2) as remaining  -- Gets the rest of the string after the first comma + space
  from kaggle.netflix_data
  where cast is not null

  union all

  select
    show_id,
    substring_index(remaining, ',', 1),
    substring(remaining, length(substring_index(remaining, ',', 1)) + 2)
  from split_cte
  where remaining != ''
)
select show_id, trim(cast) as cast
from split_cte
-- where show_id = 's100';




-- handle date_added field by converting into date field

select * from kaggle.netflix_data nd ;




-- populate missing values in country, duration columns

select * --  show_id, country 
from kaggle.netflix_data nd 
where country is null

insert into netflix_country
select nd.show_id, mapping.country
from kaggle.netflix_data nd 
inner join (select nd.director,nc.country 
from netflix_directors nd 
inner join netflix_country nc on nd.show_id  = nc.show_id 
-- where nd.director like '%Aaron Sorkin%'
group by nd.director,nc.country 
-- order by nd.director
) as mapping 
on nd.director  = mapping.director
where nd.country is null



select * from kaggle.netflix_data nd where nd.duration is null;

select * from kaggle.netflix_staging ns ;



-- Starting with Netflix Data Analysis

-- Question 1 - For each director count number of movies and number of Tv Shows

select nd.director,
	count(distinct case when ns.`type` = 'Movie' then ns.show_id end) as movie_count,
	count(distinct case when ns.`type` = 'TV Show' then ns.show_id end) as TV_Show_Count
from kaggle.netflix_staging ns 
join kaggle.netflix_directors nd on ns.show_id  = nd.show_id 
group by nd.director
having count(distinct ns.`type`) > 1
-- order by ns.`type` desc




-- Question 2: Which country has highest number of comedy movies

select nc.country ,count(ns.show_id) as number_of_movies
from kaggle.netflix_country nc 
inner join kaggle.netflix_genre ng on nc.show_id = ng.show_id 
inner join kaggle.netflix_staging ns on ns.show_id = ng.show_id 
where ng.genre = 'Comedies' and ns.`type` = 'Movie'
group by country
order by number_of_movies desc
-- limit 1;


-- Question 3: for each year(as per date added to netflix), which director hsa the maximum number of movies released

with cte as (
select nd.director,year(ns.date_added) as date_year,count(distinct ns.show_id) as number_of_movies
from kaggle.netflix_staging ns 
inner join kaggle.netflix_directors nd on ns.show_id = nd.show_id 
where ns.type = 'Movie'
group by nd.director,year(ns.date_added)
order by number_of_movies desc 
)
,cte_2 as (
select *,
		row_number() over(partition by date_year order by number_of_movies desc ) as cnt
from cte 
order by date_year, number_of_movies desc
)
select * from cte_2 
where cnt = 1;

-- Question 4 - What is the average duration of movies in each genre

select ng.genre,round(avg(cast(replace(ns.duration,'min','') as unsigned)),2) as avg_duration
from kaggle.netflix_staging ns 
inner join kaggle.netflix_genre ng on ns.show_id = ng.show_id
where ns.`type` = 'Movie'
group by ng.genre






-- Question 5 - Find directors who have created horror and comedy movies both
--     display director names along with number of comedy and horror movies directed by them


select nd.director, 
		count(distinct case when ng.genre = 'Horror Movies' then ns.show_id end) as horror_movies,
		count(distinct case when ng.genre = 'Comedies' then ns.show_id end) as Comedy_movies
from kaggle.netflix_genre ng 
inner join kaggle.netflix_directors nd on ng.show_id = nd.show_id 
inner join kaggle.netflix_staging ns on nd.show_id  = ns.show_id 
where ns.`type` = 'Movie' and ng.genre in ('Horror Movies','Comedies')
group by nd.director
having count(distinct ng.genre) = 2  -- who created both genre 'Horror Movies' and 'Comedies'


