SELECT * FROM public.nhp_rtdas_ingest
--WHERE "DateTime"::date BETWEEN '25/07/25' AND '02/08/25'
WHERE "DateTime"::date >= '26/07/25' AND "StationID" IN ('&5604C1D8','&5604CF0A','&5604D2AE','&5604DC7C','&5604E9E6')
ORDER BY "DateTime"::date ASC