from local_tools import worker1_local, worker2_local
import click


@click.command()
@click.option('--etl', '-e', default=0, help='Choose etl to run: 1 or 2?', required=True, type=int)
@click.option('--links', '-l', type=str, multiple=True, help='Only available with etl-1')
@click.option('--pages', '-p', type=int, help='Only available with etl-1')
@click.option('--offset', type=int, help='Only available with etl-2')
@click.option('--offsethigh', type=int, help='Only available with etl-2')
@click.option('--limit', type=int, help='Only available with etl-2')
def main(etl: int, links: tuple, pages: int, offset : int, offsethigh: int, limit: int):
    if etl == 0:
        worker1_local.start()
        worker2_local.start()
    elif etl == 1:
        if links and pages:
            worker1_local.start(list(links), pages)
        else:
            worker1_local.start()
    elif etl == 2:
        if offset and limit:
            if offsethigh:
                worker2_local.start(offset, offsethigh, limit)
            else:
                worker2_local.start(offset=offset, limit=limit)
        else:
            worker2_local.start()
    else:
        print("Out of option.\nThere only 3 options:\n- Run both etl: 0\n- Run etl-1: 1\n- Run etl-2: 2")

if __name__ == "__main__":
    main()