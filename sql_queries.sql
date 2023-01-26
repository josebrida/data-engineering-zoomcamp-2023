-- Count how many trips started and finished on 2019-01-15
select count(1) from green_taxi_trips
where lpep_pickup_datetime>= '2019-01-15 00:00:00' and lpep_dropoff_datetime<'2019-01-16 00:00:00'
--result = 20530

--largest trip_distance for of each day
select date_trunc('day', lpep_pickup_datetime) as day, max(trip_distance) largest_trip
from green_taxi_trips 
group by date_trunc('day', lpep_pickup_datetime)
order by largest_trip desc

-- In 2019-01-01 how many trips had 2 and 3 passengers
select passenger_count,count(1)
from green_taxi_trips 
where date_trunc('day',lpep_pickup_datetime) = '2019-01-01'
group by passenger_count

--For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
select *
from green_taxi_trips t,
	zones zpu,
    zones zdo
WHERE
    (t."PULocationID" = zpu."LocationID" AND
    t."DOLocationID" = zdo."LocationID")
	AND t."PULocationID" = 7
	AND tip_amount= (select max(tip_amount) 
					 from green_taxi_trips t, zones zpu
					 where (t."PULocationID" = zpu."LocationID") 
					 		and zpu."Zone" = 'Astoria')