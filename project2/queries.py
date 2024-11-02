queries = ["" for i in range(0, 4)]

queries[0] = """
select 0;
"""

### 1.
queries[1] = ["", ""]
### <answer1>
queries[1][0] = "count(customerid)"
### <answer2>
queries[1][1] = "flights natural left  outer join flewon_cust7"


### 2.
queries[2] = """
SELECT name, round((with portData as (select name as portName, airlineid from ((SELECT source as airportid, airlineid FROM flights union all SELECT dest as airportid, airlineid FROM flights) as airportidunion natural join airports)) 
select count(*) from portData where airlineid = 'AA' and portName = name)/(count(*) * 1.0), 2) as participation
FROM (SELECT source as airportid FROM flights union all SELECT dest as airportid FROM flights) as airportidunion
	natural join airports
GROUP BY name
ORDER BY participation DESC;
"""

### 3.
### Explaination - The query deletes flightids that have touched JFK, but not airlines.
###
queries[3] = """
SELECT airlineid 
FROM flights_airports a LEFT JOIN flights_jfk j 
	ON a.flightid = j.flightid	
WHERE airlineid not in (select distinct airlineid from flights_jfk natural join flights_airports)
GROUP BY airlineid
HAVING count(*) >= 15
ORDER BY airlineid;
"""