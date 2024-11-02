--triggers points recalculation
create or replace function pointsRec(cid text, aid text) returns integer as $$
declare points integer;
begin
    select coalesce(sum(trunc(extract(epoch from local_arrival_time - local_departing_time)/60)), 0) 
    into points
    from flewon 
    natural join flights 
    where customerid = cid AND airlineid = aid;
  return points;
end;
$$ language plpgsql;

--trigger 1 function
create or replace function cust_to_new() returns trigger as $$
declare recent text;
declare newair text;
begin

  if pg_trigger_depth() > 1 then
    return null;
  end if;

  IF (TG_OP = 'INSERT') THEN
    -- Insert into newcustomers
    INSERT INTO newcustomers(customerid, name, birthdate)
    VALUES (NEW.customerid, NEW.name, NEW.birthdate);

    -- Insert into ffairlines if frequentflieron is not NULL
    IF NEW.frequentflieron IS NOT NULL THEN
      INSERT INTO ffairlines(customerid, airlineid, points)
      VALUES (NEW.customerid, NEW.frequentflieron, pointsRec(NEW.customerid, NEW.frequentflieron)); 
    END IF;

  elsif (TG_OP = 'UPDATE') then
    update newcustomers
    set name = NEW.name, birthdate = NEW.birthdate
    where customerid = NEW.customerid;

    select NEW.frequentflieron into newair;

    if OLD.frequentflieron IS DISTINCT FROM NEW.frequentflieron THEN
      if NEW.frequentflieron IS NULL THEN
        delete from ffairlines where customerid = OLD.customerid;
      else
        -- insert into ffairlines(customerid, airlineid, points)
        -- values (NEW.customerid, newair, 0);

        --if it is not in ffairlines, then you need to pick the alphabetically sorted one
        if NEW.frequentflieron not in (select airlineid from ffairlines where customerid = NEW.customerid) then

            insert into ffairlines(customerid, airlineid, points)
            values (NEW.customerid, newair, pointsRec(NEW.customerid, NEW.frequentflieron));
            -- If not in flewon, will return null
            select fl.airlineid into recent
            from flewon f
            join flights fl on fl.flightid = f.flightid
            where f.customerid = NEW.customerid
            and fl.airlineid in (select airlineid from ffairlines where customerid = NEW.customerid)
            order by f.flightdate desc nulls last, fl.airlineid asc
            limit 1;

            
            if recent is null then -- not inside flewon
                select airlineid into recent
                from ffairlines
                where customerid = new.customerid
                order by airlineid asc
                limit 1;
            end if;
            -- Revert frequentflieron to the valid airline
            update customers
            set frequentflieron = recent
            where customerid = NEW.customerid;
        end if;
      end if;
    
    end if;

  elsif (TG_OP = 'DELETE') then
    delete from newcustomers where customerid = old.customerid;
    delete from ffairlines where customerid = old.customerid;
  end if;
  return null;
end;
$$ language plpgsql;

--trigger 1 attachment
create trigger sync_customers
after INSERT or UPDATE or DELETE on customers
for each row execute function cust_to_new();

--trigger 2 function
create or replace function new_to_cust() returns trigger as $$
declare recent text; -- variable to store most recent airline
begin
  if pg_trigger_depth() > 1 then
      return null;
  end if;

  if (TG_OP = 'INSERT') then
    insert into customers(customerid, name, birthdate, frequentflieron)
    --
    values (new.customerid, new.name, new.birthdate, NULL); 

  elsif (TG_OP = 'UPDATE') then
    update customers
    set name = new.name, birthdate = new.birthdate
    where customerid = new.customerid;

    --check what the most recent airline is
    select fl.airlineid into recent
    from flewon f
    join flights fl on fl.flightid = f.flightid
    where f.customerid = new.customerid
    and exists (select 1 from ffairlines ff where ff.airlineid = fl.airlineid and ff.customerid = f.customerid) -- Only consider airlines in ffairlines
    order by f.flightdate desc nulls last, fl.airlineid asc
    limit 1;

    update customers
    set frequentflieron = coalesce(recent, NULL)
    where customerid = new.customerid;

  elsif (TG_OP = 'DELETE') then
    delete from customers where customerid = old.customerid;
    --delete from ffairlines where customerid = old.customerid;
  end if;
  return null;
end;
$$ language plpgsql;

--trigger 2 attachment
create trigger sync_newcust
after INSERT or UPDATE or DELETE on newcustomers
for each row execute function new_to_cust();


--trigger 3 function
create or replace function ffa_to_cust() returns trigger as $$
declare recent text;
begin
  --this might not be what we want, but lets see if this is our issue
    if pg_trigger_depth() > 1 then
      return null;
    end if;
  if(TG_OP = 'INSERT' OR TG_OP = 'UPDATE') then
   select fl.airlineid into recent
        from flewon f
        join flights fl on fl.flightid = f.flightid
        where f.customerid = new.customerid 
        and fl.airlineid in (select airlineid from ffairlines where customerid = new.customerid) -- Only consider airlines in ffairlines
        order by f.flightdate desc, fl.airlineid asc
        limit 1;

        -- Update customers with the most recent airline or NULL
        update customers
        set frequentflieron = coalesce(recent, NULL)
        where customerid = new.customerid;
  elsif (TG_OP = 'DELETE') then

      if not exists (select 1 from ffairlines where customerid = old.customerid) then
        update customers
        set frequentflieron = NULL
        where customerid = old.customerid;
      else
        select fl.airlineid into recent
        from flewon f
        join flights fl on fl.flightid = f.flightid
        where f.customerid = old.customerid 
        and fl.airlineid in (select airlineid from ffairlines where customerid = old.customerid) -- Only consider airlines in ffairlines
        order by f.flightdate desc, fl.airlineid asc
        limit 1;


        -- Update customers with the most recent airline or NULL
        update customers
        set frequentflieron = coalesce(recent, NULL)
        where customerid = old.customerid;
      end if;
  end if;

  return null;
end;
$$ language plpgsql;

--trigger 3 attachment
create trigger sync_ffa
after insert or update or delete on ffairlines
for each row execute function ffa_to_cust();





--trigger 4 function
create or replace function  flew_to_cust() returns trigger as $$
declare recent text;
declare airid text;
begin
    --Not sure if we want this here
    if pg_trigger_depth() > 1 then
      return null;
  end if;

  if(TG_OP = 'INSERT') then
      --init variable
      select fl.airlineid from flights fl join flewon on fl.flightid = new.flightid into airid;
  elsif(TG_OP = 'UPDATE') then
    --init variable
      select fl.airlineid from flights fl join flewon on fl.flightid = old.flightid into airid;
  end if;
  
  if(TG_OP = 'INSERT' OR TG_OP = 'UPDATE') then

  --check what the most recent airline is
      select fl.airlineid into recent
      from flewon f
      join flights fl on fl.flightid = f.flightid
      where f.customerid = new.customerid 
      and exists (select 1 from ffairlines ff where ff.airlineid = fl.airlineid and ff.customerid = f.customerid)
      order by f.flightdate desc, fl.airlineid asc
      limit 1;

  --if this most recent airline has no equivalent value in ffairlines
      update customers
      set frequentflieron = coalesce(recent, NULL)
      where customerid = new.customerid;

      -- update corresponding ffairlines entry if exists
      update ffairlines
      set points = pointsRec(new.customerid, airid)
      where customerid = new.customerid and airlineid = airid;


  elsif (TG_OP = 'DELETE') then
      --init variable
      select fl.airlineid from flights fl join flewon on fl.flightid = old.flightid into airid;

      --check what the most recent airline is
      select fl.airlineid into recent
      from flewon f
      join flights fl on fl.flightid = f.flightid
      where f.customerid = old.customerid 
      and exists (select 1 from ffairlines ff where ff.airlineid = fl.airlineid and ff.customerid = f.customerid)
      order by f.flightdate desc, fl.airlineid asc
      limit 1;

      if recent is null then -- not inside flewon
        select airlineid into recent
        from ffairlines
        where customerid = old.customerid
        order by airlineid asc
        limit 1;
      end if;

  --if this most recent airline has no equivalent value in ffairlines
      update customers
      set frequentflieron = coalesce(recent, NULL)
      where customerid = old.customerid;


      -- update corresponding ffairlines entry if exists
      update ffairlines
      set points = pointsRec(old.customerid, airid)
      where customerid = old.customerid and airlineid = airid;
  end if;
  return null;
end;
$$ language plpgsql;

--trigger 4 attachment
create trigger sync_flewon
after insert or update or delete on flewon
for each row execute function flew_to_cust();

