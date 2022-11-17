import asyncio
import logging

import click

from memkv.server.server import Server

log_format = "%(asctime)s::%(levelname)s::%(name)s::%(filename)s::%(lineno)d::%(message)s"
logging.basicConfig(
    level="INFO",
    format=log_format
)
logger = logging.getLogger(__name__)


@click.command()
@click.option("--port", default=9001, type=int, help="The port the server is listening on")
@click.option(
    "--worker-count", default=10, type=int, help="The number of workers to start the server with"
)
@click.option("--debug", is_flag=True, default=False, help="Set this to enable debug logging")
def main(port: int, worker_count: int, debug: bool):
    try:
        if debug:
            logger.setLevel("DEBUG")

        logger.info(f"Starting up server on port {port}")
        server = Server(worker_count=worker_count, port=port)
        asyncio.run(server.run())
        logger.info(f"Server is executing gracefully")
    except KeyboardInterrupt:
        server.terminate()
    finally:
        server.terminate()


if __name__ == "__main__":
    main()
