# ShopeeCrawler
Crawl data from Shopee and analyze 

## Instructions
Using docker

```cli
docker compose run scheduler --name <name_of_service> --entrypoint -d \

add_job \
# or add_job <yml file> \ 

run_job \
# or run_job now \

remove_job \
# or remove_job <job_id> \
```
