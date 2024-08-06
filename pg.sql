CREATE SCHEMA report;
CREATE SCHEMA sync;

drop table report.load_qty_tare_hour;
CREATE TABLE IF NOT EXISTS report.load_qty_tare_hour
(
    dt_load   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    dt_hour   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    office_id INT                         NOT NULL,
    qty_load  BIGINT                      NOT NULL,
    PRIMARY KEY (dt_hour, office_id)
);

CREATE OR REPLACE FUNCTION report.load_qty_tare_hour(_date DATE) RETURNS JSONB
    SECURITY DEFINER
    LANGUAGE plpgsql
AS
$$
BEGIN
    SET TIME ZONE 'Europe/Moscow';

    RETURN JSONB_BUILD_OBJECT('data', JSONB_AGG(ROW_TO_JSON(res)))
        FROM (SELECT dt_hour,
                     office_id,
                     qty_load
              FROM report.load_qty_tare_hour a
              WHERE a.dt_hour >= _date::TIMESTAMP
                AND a.dt_hour < _date::TIMESTAMP + interval '1 day'
              ) res;
END;
$$;


CREATE OR REPLACE PROCEDURE sync.load_qty_tare_hour_importfromclick(_src JSONB)
    SECURITY DEFINER
    LANGUAGE plpgsql
AS
$$
BEGIN
    WITH cte AS (SELECT DISTINCT ON (src.dt_hour, src.office_id) src.dt_load,
                                                                 src.dt_hour,
                                                                 src.office_id,
                                                                 src.qty_load
                 FROM JSONB_TO_RECORDSET(_src) AS src(dt_load   TIMESTAMP WITHOUT TIME ZONE,
                                                      dt_hour   TIMESTAMP WITHOUT TIME ZONE,
                                                      office_id INT,
                                                      qty_load  BIGINT)
                 ORDER BY src.dt_hour, src.office_id, src.dt_load DESC)
    INSERT
    INTO report.load_qty_tare_hour AS asd(dt_load,
                                          dt_hour,
                                          office_id,
                                          qty_load)
    SELECT c.dt_load,
           c.dt_hour,
           c.office_id,
           c.qty_load
    FROM cte c
    ON CONFLICT (dt_hour, office_id) DO UPDATE
        SET qty_load = excluded.qty_load
    WHERE asd.dt_load < excluded.dt_load;
END;
$$;

select * from report.load_qty_tare_hour;
