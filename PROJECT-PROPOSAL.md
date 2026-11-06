## Project Proposal

### Why

As I'm set to do my co-op at NetSkrt in January, I wanted to learn a bit more about their systems and business problem. So I planning to do a small scale edge CDN with caching proxy for HLS video streaming, and for the 7171 component, a Star Schema data warehouse that handles delivery logs for analytics to optimize cache efficiency for the 8071 component.

### What

I'm wanting to do a project on CDNs, with a focus in video streaming. The goal of the project is simulate a edge CDN. The plan for this is to develop a Edge Cache Node, that uploads data to the analytics warehouse, and test it with some simulators. Everything that would be a hardware component is going to be represented as a docker container, to reenact the bare metal that would be in place. I likely won't create enough data from testing to populate the data warehouse adequately so I likely seed it from some other source.

### Major Deliverables

- Origin Server -> to act as the sender/source for the content that's contained on the edge cache
- Edge Cache Node -> where the actual caching takes place and the source of the data for the data warehouse
- Receiver Server -> likely just some scripting, like Jmeter to act as a bunch of users
- Data warehouse

1. What Data Ware House you are creating?

> A CDN Video Cache and Streaming Analytics warehouse

2. Why?  list the associations/correlations/classifications/trends you want to learn.

> Correlations/Associations
>
> - Correlations between playback time and viewer drop-off rate  
> - Correlations between when a cache should expire and traffic against  
> - Associations between demand and genre  

> Classifications
>
> - Content and quality
> - Temporal Windows

> Trends
>
> - Demand during peak hours

3. Capture the above needs in the form of OLAP queries (in simple English).  Just couple of queries would be sufficient.

> What is the total bandwidth save for each cache hit of each video
> how many unique viewers reached each successive chunk segment, and at what segment index does the sharpest viewer drop-off happen

4. OLAP Schema(s) of your data warehouse

>fact_chunk_delivery
>
> • delivery_id
> • time_id
> • content_id
> • client_id
> • eviction_policy
> • is_cache_hit
> • bytes_used

> • dim_time: time_id , timestamp, hour, minute
> • dim_content: content_id, video_id, video_title, genre, duration_sec, bitrate
> • dim_client: client_id, session_id

5. SQL representation of your OLAP queries

```sql
select
  dc.video_title,
  dt.hour,
  count(*)                                         
  sum(f.is_cache_hit)                              
  round(100.0 * sum(f.is_cache_hit) / count(*), 1) 
from fact_chunk_delivery f
join dim_content dc on f.content_key = dc.content_key
join dim_time dt on f.time_key = dt.time_key
group by rollup (dc.video_title, dt.hour)
order by dc.video_title nulls last, dt.hour nulls last;
```

6. Data source(s) for your data warehouse

> Kaggle, youtube, etc, if I need to, otherwise `ffmpeg` and tracking setup from the CDN

7. Technology to be used used in the creation of your data engineering solution including for ETL, backend server and database, and plotting/reporting

> Python/Pyspark For ETL, backend in rust.
