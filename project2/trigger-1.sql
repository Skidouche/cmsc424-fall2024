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
    BEGIN

        IF pg_trigger_depth() > 1 THEN
            return NULL;
        END IF;

        -- On frequentflieron update, insert updated point tuple into ffairlines, but if null, then delete all corresponding customerids from frequentflieron
        IF TG_OP = 'UPDATE' THEN
            IF (select frequentflieron from NEW) IS NOT NULL THEN
                -- On change to frequentflieron
                IF (select frequentflieron from NEW) <> (select frequentflieron from OLD) THEN
                    -- If new frequentflieron is not in ffairlines
                    IF NEW.frequentflieron not in (select airlineid from ffairlines where customerid = NEW.customerid) THEN
                        INSERT INTO ffairlines(customerid, airlineid, points) values (NEW.customerid, NEW.frequentflieron, get_points(NEW.customerid, NEW.frequentflieron));
                        IF EXISTS (select * from flewon natural join flights where customerid = NEW.customerid and airlineid in (select airlineid from ffairlines where customerid = NEW.customerid) order by flightdate desc, airlineid asc LIMIT 1) THEN
                            UPDATE customers
                                SET frequentflieron = (select airlineid from flewon natural join flights where customerid = NEW.customerid and airlineid in (select airlineid from ffairlines where customerid = NEW.customerid) order by flightdate desc, airlineid asc LIMIT 1)
                                WHERE customerid = NEW.customerid;
                        ELSE 
                            UPDATE customers
                                SET frequentflieron = (select airlineid from ffairlines where customerid = new.customerid order by airlineid limit 1)
                                WHERE customerid = NEW.customerid;
                        END IF;
                    END IF;
                END IF;  
            ELSE 
                DELETE FROM ffairlines WHERE ffairlines.customerid = OLD.customerid;
            END IF;
            
            UPDATE newcustomers
                set name = NEW.name, birthdate = NEW.birthdte where customerid = NEW.customerid;

        END IF;

        -- On customers insert, insert into ffairlines  (customerid, frequentflieron, points) if frequentflieron is not null
        IF TG_OP = 'INSERT' THEN
            --INSERT INTO newcustomers(customerid, name, birthdate) values (NEW.customerid, NEW.name, NEW.birthdate);
            --IF NEW.frequentflieron IS NOT NULL THEN
             --   INSERT INTO ffairlines(customerid, airlineid, points) values (NEW.customerid, NEW.frequentflieron, get_points(NEW.customerid, NEW.frequentflieron)); -- Implied that new flier has no points
            --END IF;
            IF NEW.frequentflieron IS NOT NULL THEN
                INSERT INTO ffairlines(customerid, airlineid, points)
                VALUES (NEW.customerid, NEW.frequentflieron, pointsRec(NEW.customerid, NEW.frequentflieron)); 
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

CREATE TRIGGER update_cust
    AFTER INSERT OR UPDATE OR DELETE
        ON customers
    FOR EACH ROW 
        EXECUTE PROCEDURE customer_update();
