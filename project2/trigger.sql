-- trigger.sql
CREATE OR REPLACE FUNCTION get_points(custID text, airID text) returns integer as $$
DECLARE points integer;
BEGIN
    select coalesce(sum((extract(epoch FROM (local_arrival_time - local_departing_time))/60))::int, 0) into points
    from flewon natural join flights 
    where customerid = custID and airlineid = airid;
    return points;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION customer_update() RETURNS trigger AS $$
    DECLARE latest text;
    BEGIN

        IF pg_trigger_depth() > 1 THEN
            return NULL;
        END IF;

        --On frequentflieron update, insert updated point tuple into ffairlines, but if null, then delete all corresponding customerids from frequentflieron
        IF TG_OP = 'UPDATE' THEN
            UPDATE newcustomers SET name = NEW.name,  birthdate = NEW.birthdate where customerid = NEW.customerid;
            
            IF (NEW.frequentflieron) IS NOT NULL THEN
                -- On change to frequentflieron
                IF (NEW.frequentflieron) <> (OLD.frequentflieron) THEN
                    -- If new frequentflieron is not in ffairlines
                    IF NEW.frequentflieron not in (select airlineid from ffairlines where customerid = NEW.customerid) THEN
                        INSERT INTO ffairlines(customerid, airlineid, points) values (NEW.customerid, NEW.frequentflieron, get_points(NEW.customerid, NEW.frequentflieron));
                        select airlineid into latest from flewon natural join flights where customerid = NEW.customerid and airlineid in (select airlineid from ffairlines where customerid = NEW.customerid) order by flightdate desc, airlineid asc LIMIT 1;
                        IF latest is NULL THEN
                            select airlineid into latest from ffairlines where customerid = new.customerid order by airlineid limit 1;
                        END IF;
                        UPDATE customers SET frequentflieron = latest WHERE customerid = NEW.customerid;
                    END IF;
                END IF;  
            ELSE 
                DELETE FROM ffairlines WHERE customerid = OLD.customerid;
            END IF;

        END IF;

        -- On customers insert, insert into ffairlines  (customerid, frequentflieron, points) if frequentflieron is not null
        IF TG_OP = 'INSERT' THEN
            INSERT INTO newcustomers(customerid, name, birthdate) values (NEW.customerid, NEW.name, NEW.birthdate);
            IF NEW.frequentflieron IS NOT NULL THEN
                INSERT INTO ffairlines(customerid, airlineid, points) values (NEW.customerid, NEW.frequentflieron, get_points(NEW.customerid, NEW.frequentflieron)); -- Implied that new flier has no points
            END IF;
        END IF;

        -- On customers delete, delete all corresponding customerids from ffairlines
        IF TG_OP = 'DELETE' THEN
            DELETE FROM ffairlines WHERE ffairlines.customerid = OLD.customerid;
            DELETE FROM newcustomers WHERE newcustomers.customerid = OLD.customerid;
        END IF;
        
        return NULL;

    END;
$$ LANGUAGE plpgsql;

-- Update customers, ffairlines accordingly to newcustomers updates
CREATE OR REPLACE FUNCTION newcustomer_update() RETURNS trigger AS $$
    BEGIN

        IF pg_trigger_depth() > 1 THEN
            return NULL;
        END IF;
    
        -- On insert, check if it already exists in customers. If so,update information. If not, insert. Update ffairlines secondarily 
        IF TG_OP = 'INSERT' THEN
            INSERT INTO customers(customerid, name, birthdate, frequentflieron) values (NEW.customerid, NEW.name, NEW.birthdate, NULL); 
        END IF;

        IF TG_OP = 'DELETE' THEN
            DELETE FROM customers WHERE customers.customerid = OLD.customerid;
        END IF;

        IF TG_OP = 'UPDATE' THEN
            UPDATE customers SET name = NEW.name, birthdate = new.birthdate where customerid = NEW.customerid;
            UPDATE customers SET frequentflieron = coalesce((select airlineid from flewon natural join flights where customerid = NEW.customerid and airlineid in (select airlineid from ffairlines where customerid = NEW.customerid) order by flightdate desc, airlineid limit 1), NULL) WHERE customerid = NEW.customerid;
        END IF;

        return NULL;

    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ff_update() RETURNS trigger as $$
BEGIN

    IF pg_trigger_depth() > 1 THEN
        return NULL;
    END IF;

    IF (TG_OP = 'UPDATE') or (TG_OP = 'INSERT') THEN
        UPDATE customers SET frequentflieron = coalesce((select airlineid from flewon natural join flights where customerid = NEW.customerid and airlineid in (select airlineid from ffairlines where customerid = NEW.customerid) order by flightdate desc, airlineid limit 1), NULL) WHERE customerid = NEW.customerid;
    END IF;

    IF TG_OP = 'DELETE' THEN
        IF EXISTS (select * from ffairlines where customerid = OLD.customerid) THEN
            UPDATE customers SET frequentflieron = coalesce((select airlineid from flewon natural join flights where customerid = OLD.customerid and airlineid in (select airlineid from ffairlines where customerid = OLD.customerid) order by flightdate desc, airlineid limit 1), NULL) WHERE customerid = OLD.customerid;
        ELSE
            UPDATE customers SET frequentflieron = NULL where customerid = OLD.customerid;
        END IF;
    END IF;

    return NULL;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION flewon_update() returns trigger as $$
DECLARE airline text;
BEGIN

    IF pg_trigger_depth() > 1 THEN
        return NULL;
    END IF;

    IF TG_OP = 'DELETE' THEN
        SELECT airlineid FROM flights fli join flewon on fli.flightid = OLD.flightid into airline;

        IF EXISTS (select * from flewon natural join flights where customerid = OLD.customerid and airlineid in (select airlineid from ffairlines where customerid = OLD.customerid)) THEN
            UPDATE customers set frequentflieron = coalesce((select airlineid from flewon x natural join flights where customerid = OLD.customerid and airlineid in (select airlineid from ffairlines where customerid = x.customerid) ORDER BY flightdate desc, airlineid limit 1), NULL) where customerid = OLD.customerid;
        ELSE
            UPDATE customers set frequentflieron = coalesce((select airlineid from ffairlines where customerid = OLD.customerid order by airlineid limit 1), NULL) where customerid = OLD.customerid;
        END IF;

        UPDATE ffairlines SET points = get_points(old.customerid, airline) WHERE customerid = old.customerid and airlineid = airline;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        UPDATE customers set frequentflieron = coalesce((select airlineid from flewon x natural join flights where customerid = NEW.customerid and airlineid in (select airlineid from ffairlines where customerid = x.customerid) ORDER BY flightdate desc, airlineid limit 1), null) where customerid = NEW.customerid;
        with airline as (select substring(OLD.flightid, 1, 2))
            UPDATE ffairlines set points = get_points(new.customerid, (select * from airline)) where customerid = new.customerid and airlineid = (select * from airline);
    END IF;

    IF TG_OP = 'INSERT' THEN
        UPDATE customers set frequentflieron = coalesce((select airlineid from flewon x natural join flights where customerid = NEW.customerid and airlineid in (select airlineid from ffairlines where customerid = x.customerid) ORDER BY flightdate desc, airlineid limit 1), null) where customerid = NEW.customerid;
        with airline as (select substring(NEW.flightid, 1, 2))
            UPDATE ffairlines set points = get_points(new.customerid, (select * from airline)) where customerid = new.customerid and airlineid = (select * from airline);
    END IF;

    return NULL;

END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_cust
    AFTER INSERT OR UPDATE OR DELETE
        ON customers
    FOR EACH ROW 
        EXECUTE PROCEDURE customer_update();

CREATE TRIGGER update_newcust
    AFTER INSERT OR UPDATE OR DELETE
        ON newcustomers
    FOR EACH ROW
        EXECUTE PROCEDURE newcustomer_update();

CREATE TRIGGER update_ff
    AFTER INSERT OR UPDATE OR DELETE
        ON ffairlines
    FOR EACH ROW
        EXECUTE PROCEDURE ff_update();
        
CREATE TRIGGER update_flewon
    AFTER INSERT OR UPDATE OR DELETE
        ON flewon
    FOR EACH ROW
        EXECUTE PROCEDURE flewon_update();