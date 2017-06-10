# dataflow-batch-snippets

python script use apache-beam and Google Cloud Platform Dataflow.

----

# (1) geoip 

BigQuery Table which include IP List to dest table which mapped GeoIP Country.

execute like this

```bash
python geoip_mapper.py googleProjectId src_dataset src_tablename dest_dataset dest_tablename gs://test/ testjobname  
```

## BigQuery Table Data Example 
 
### Source Table 

| ip             |  accessTime              |
|----------------|--------------------------|  
| 112.175.60.132 | 2017-06-10 17:51:46 UTC  |
| 216.58.197.206 | 2017-06-10 18:12:26 UTC  |

### Result Table

| ip             |  accessTime              |  GeoIpCountry  |
|----------------|--------------------------|----------------|
| 112.175.60.132 | 2017-06-10 17:51:46 UTC  |  KR            |
| 216.58.197.206 | 2017-06-10 18:12:26 UTC  |  US            |


----

This product includes GeoLite data created by MaxMind, available from 
<a href="http://www.maxmind.com">http://www.maxmind.com</a>.

