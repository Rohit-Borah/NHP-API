SELECT * FROM public.nhp_rtdas_ingest
WHERE "StationID" !~ '^&[a-fA-F0-9]{8}$'
  
SELECT * FROM public.nhp_rtdas_ingest
WHERE "DateTime" !~ '^\d{2}[-/]\d{2}[-/]\d{2} \d{2}:\d{2}:\d{2}$'
   AND "DateTime" !~ '^\d{2}[-/]\d{2}[-/]\d{2} \d{2}:\d{2}$';

