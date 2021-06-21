from local_tools import worker1_local, worker2_local
import click


@click.command()
@click.option('--etl', '-e', help='Choose etl to run: 1 or 2?', required=True, type=int)
def main(etl):
    if etl == 1:
        worker1_local.start()
    elif etl == 2:
        worker2_local.start()
    else:
        print("Out of option.\nThere only 2 options:\n- Run etl-1: 1\n- Run etl-2: 2")
if __name__ == "__main__":
    main()