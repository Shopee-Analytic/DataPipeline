from scheduler import run_job, add_job, remove_job
import sys


# docker compose run scheduler -d --entrypoint \

# add_job \
# add_job <yml file> \ 

# run_job \
# run_job now \

# remove_job \
# remove_job <job_id> \

def main(args):
    if "add_job" in args:
        try:
            file = args[args.index("add_job") + 1]
            if file.split(".")[-1] in ("yml", "yaml"):
                add_job(file)
        except Exception:
            add_job()
    elif "run_job" in args:
        if "now" in args:
            run_job(now=True)
        else:
            run_job()
    elif "remove_job" in args:
        remove_job()

if __name__ == "__main__":
    main(sys.argv)



# Funtion:
#   - add_job (all) by config.yml
#   - remove_job (one by _id or all)
#   - run_job (one by _id or all)