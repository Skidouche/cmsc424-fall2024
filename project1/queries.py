queries = ["" for i in range(0, 12)]
### EXAMPLE
### 0. List all airport codes and their cities. Order by the city name in the increasing order.
### Output column order: airportid, city

queries[0] = """
select airportid, city
from airports
order by city;
"""

### 1. Write a query to find the names of customers who have flights on a Monday and 
###    first name that has a second letter is not a vowel [a, e, i, o, u].
###    If a customer who satisfies the condition flies on multiple Mondays, output their name only once.
###    Do not include the oldest customer among those that satisfies the above conditions in the results.
### Hint:  - See postgresql date operators that are linked to from the README, and the "like" operator (see Chapter 3.4.2). 
###        - Alternately, you can use a regex to match the condition imposed on the name.
###        - See postgresql date operators and string functions
###        - You may want to use a self-join to avoid including the oldest customer.
###        - When testing, write a query that includes all customers, then modify that to exclude the oldest.
### Order: by name
### Output columns: name
queries[1] = """
select distinct name 
from customers natural join flewon 
where extract(dow from flightdate) = 1 and name !~ '^.(a|e|i|o|u).*' and birthdate != (select min(birthdate) from customers)
order by name;
"""

### 2. Write a query to find customers who are frequent fliers on Delta Airlines (DL) 
###    and have their birthday are either before 02/15 or after 11/15 (mm/dd). 
### Hint: See postgresql date functions.
### Order: by birthdate
### Output columns: customer id, name, birthdate
queries[2] = """
select customerid, name, birthdate
from customers
where frequentflieron like 'DL' and not (select extract(doy from birthdate) between 46 and 319)
order by birthdate;
"""

### 3. Write a query to rank the customers who have taken most number of flights with their
###    frequentflieron airline, along with their name, airlineid, and number of times they 
###    have flown with the airlines. If any ties make the top 10 rankings exceed 10 results 
###    (ex. the number of most flights is shared by 20 people), list all such customers.
### Output: (rank, name, airlineid count)
### Order: rank, name
### HINT: You can use self join to rank customers based on the number of flights.
queries[3] = """
with counted as
(select customerid, count(*) from customers natural join flewon natural join flights where frequentflieron = airlineid group by customerid order by count desc, name),

ranked as
(select *, (select count(*) + 1 from counted where count >  c.count) as rank from counted c)


select rank, name, frequentflieron, count from ranked natural join customers where rank <= 10;

"""


### 4. Write a query to find the airlines with the least number of customers that 
###    choose it as their frequent flier airline. For example, if 10 customers have Delta
###    listed as their frequent flier airline, and no other airlines have fewer than 10
###    frequent flier customers, then the query should return  "DELTA, 10" as the
###    only result. In the case of a tie, return all tied airlines.
### Hint: use `with clause` and nested queries (Chapter 3.8.6). 
### Output: name, count
### Order: name
queries[4] = """
with lineCount as
(select frequentflieron, count(*) from customers group by frequentflieron),

leastFlown as
(select frequentflieron as airlineid, count from lineCount where count = (select min(count) from lineCount))

select name, count from airlines natural join leastFlown order by name;

"""


### 5. Write a query to find the most-frequent flyers (customers who have flown on most number of flights).
###    In this dataset and in general, always assume that there could be multiple flyers who satisfy this condition.
###    Assuming multiple customers exist, list the customer names along with the count of other frequent flyers
###    they have flown with.
###    Two customers are said to have flown together when they have a flewon entry with a matching flightid and flightdate.
###    For example if Alice, Bob and Charlie flew on the most number of flighs (3 each). Assuming Alice and Bob never flew together,
###    while Charlie flew with both of them, the expected output would be: [('Alice', 1), ('Bob', 1), ('Charlie', 2)].
### NOTE: A frequent flyer here is purely based on number of occurances in flewon, (not the frequentflieron field).
### Output: name, count
### Order: order by count desc, name.
queries[5] = """

with flycount as (select customerid, count(*) from customers natural join flewon group by customerid),
mostFlown as (select name, customers.customerid, flightid, flightdate from customers natural join flycount natural join flewon where count = (select max(count) from flycount)),
uniquePairs as (select distinct a.name as name1, b.name as name2 from mostFlown a, mostFlown b where (a.flightid = b.flightid) and (a.flightdate = b.flightdate) and (a.name != b.name))
select name1, count(*) from uniquePairs group by name1 order by count desc, name1;
"""

# wtf is going on with shared flights like huh???

### 6. Write a query to find the percentage participation of American Airlines in each airport, relative to the other airlines.
### One instance of participation in an airport is defined as a flight (EX. AA150) having a source or dest of that airport.
### If UA101 leaves OAK and arrives in DFW, that adds 1 to American's count for both OAK and DFW airports.
### This means that if AA has 1 in DFW, UA has 1 in DFW, DL has 2 in DFW, and SW has 3 in DFW, the query returns:
###     airport 		                              | participation
###     General Edward Lawrence Logan International   | .14
### Output: (airport_name, participation).
### Order: Participation in descending order, airport name
### Note: - The airport column must be the full name of the airport
###       - The participation percentage is rounded to 2 decimals, as shown above
###       - You do not need to confirm that the flights actually occur by referencing the flewon table. This query is only concerned with
###         flights that exist in the flights table.
###       - You must not leave out airports that have no UA flights (participation of 0)
queries[6] = """

with portFlightData as (select name, source, dest, flightid, airlineid from airports, flights where (source = airportid or dest = airportid) order by name),
AAFlights as (select distinct  name, (select count (*) from portFlightData where airlineid like 'AA' and name = a.name group by a.name) as AACount from portFlightData a),
ratios as (select name, ((aacount * 1.0)/(totalflights * 1.0)) as ratio from AAFlights natural join (select name, count(*) as totalFlights from portFlightData group by name) as totals),
concatenated as ((select name, 0.0 as ratio from ratios where ratio = 1 is unknown) union (select name, ratio from ratios where ratio = 1 is not unknown) order by desc)
"""
# Convert unknowns to 0s
# round

### 7. Write a query to find the customer/customers that taken the highest number of flights but have never flown on their frequentflier airline.
###    If there is a tie, return the names of all such customers. 
### Output: Customer name
### Order: name
queries[7] = """

with unloyals as (select name, customerid from customers where frequentflieron not in (select distinct airlineid from flights natural join flewon where customers.customerid = flewon.customerid)),
max_unloyals as (select name, count(*) from unloyals natural join flewon group by name order by count desc, name)
select name from max_unloyals where count  = (select max(count) from max_unloyals);

"""

### 8. Write a query to find customers that took the same flight (identified by flightid) on consecutive days.
###    Return the name, flightid start date and end date of the customers flights.
###    The start date should be the first date of the pair and the end date should be the second date of the pair.
###    If a customer took the same flight on multiple pairs of consecutive days, return all the pair.
###    For instance if 'John Doe' flew on UA101 on 08/01/2024, 08/02/2024, 08/03/2024, 08/06/2024, and 08/07/2024,
###    the output should be: 
###    [(John Doe ', 'UA101 ', datetime.date(2016, 8, 1), datetime.date(2016, 8, 2)),
###     (John Doe ', 'UA101 ', datetime.date(2016, 8, 2), datetime.date(2016, 8, 3)),
###     (John Doe ', 'UA101 ', datetime.date(2016, 8, 6), datetime.date(2016, 8, 7))]
### Output: customer_name, flightid, start_date, end_date
### Order: by customer_name, flightid, start_date
queries[8] = """
with multiflights as (select * from (select customerid, flightid from (select customerid, flightid, count(*) from flewon group by flightid, customerid order by customerid) as a where count > 1) as b natural join flewon order by customerid, flightid, flightdate)
select name, flightid, start, consecFlights.end from customers natural join (select a.customerid, a.flightid, a.flightdate as start, b.flightdate as end from multiFlights a, multiFlights b where (a.customerid = b.customerid) and (a.flightid = b.flightid) and (b.flightdate - a.flightdate = 1)) as consecFlights order by name, flightid, start;

"""
# query everyone that flew on a flight multiple times
# join on self where name is same, flight is same, check if start date is day after with a difference function


### 9. A layover consists of set of two flights where the destination of the first flight is the same 
###    as the source of the second flight. Additionally, the arrival of the first flight must be before the
###    departure of the first flight. 
###    Write a query to find all pairs of flights belonging to the same airline that had a layover in IAD
###    between 1 and 4 hours in length (inclusive).
### Output columns: 1st flight id, 2nd flight id, source city, destination city, layover duration
### Order by: layover duration
queries[9] = """

select a.flightid as Flight1, b.flightid as Flight2, a.source, b.dest, b.local_departing_time - a.local_arrival_time as layover_time from flights a, flights b where (a.dest like 'IAD') and (b.source like 'IAD') and (a.airlineid = b.airlineid) and ((select extract(hour from (b.local_departing_time - a.local_arrival_time)) between 1 and 3) or ((b.local_departing_time - a.local_arrival_time) = '04:00:00')) order by layover_time;

"""

# query flights where dest matches source
# query for time diff of 1 to 4 hours


### 10. Provide a ranking of the airlines that are most loyal to their hub. 
###     The loyalty of an airline to its hub is defined by the ratio of the number
###     of flights that fly in or out of the hub versus the total number of flights
###     operated by airline. 
###     Output: (name, rank)
###     Order: rank, name
### Note: a) If two airlines tie, then they should both get the same rank, and the next rank should be skipped. 
### For example, if the top two airlines have the same ratio, then there should be no rank 2, e.g., 1, 1, 3 ...
queries[10] = """

with hubCount as (select name, count(*) as hubFlights from airlines natural join flights where (source like hub) or (dest like hub) group by name),
totals as (select name, count(*) as totalFlights from airlines natural join flights group by name),
ratio as (select name, (hubFlights * 1.0)/(totalFlights * 1.0) as percentage from hubCount natural join totals)
select (select count(*) + 1 from ratio where percentage >  r.percentage) as rank, name from ratio r order by rank;

"""


### 11. OPTIONAL Query (0 Points): A (fun) challenge for you to try out. 
###    This query is a modification of query 8.
###    Write a query to find customers that took the same flight (identified by flightid) on consecutive days.
###    Return the name, flightid start and end date of the customers flights.
###    The start date should be the first date of the sequence and the end date should be the last date of the sequence.
###    If a customer took the same flight on multiple sequences of consecutive days, return all the sequences.
###    For instance if 'John Doe' flew on UA101 on 08/01/2024, 08/02/2024, 08/03/2024, 08/06/2024, and 08/07/2024,
###    the output should be: 
###    [(John Doe ', 'UA101 ', datetime.date(2016, 8, 1), datetime.date(2016, 8, 3)),
###     (John Doe ', 'UA101 ', datetime.date(2016, 8, 6), datetime.date(2016, 8, 7))]
### Output: customer_name, flightid, start_date, end_date
### Order: by customer_name, flightid, start_date
queries[11] = """
"""
