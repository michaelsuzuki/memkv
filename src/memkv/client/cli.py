import logging
import re
import shlex
from typing import Dict, List

import click
from prompt_toolkit import PromptSession, print_formatted_text
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.history import InMemoryHistory

from memkv.client.api import Client

version = "0.1"

log_format = "%(asctime)s::%(levelname)s::%(name)s::%(filename)s::%(lineno)d::%(message)s"
logging.basicConfig(
    level="INFO",
    format=log_format
)
logger = logging.getLogger(__name__)

cmd_pat = re.compile(r"^\s*(GET|SET|DELETE|METRICS)\s*", re.IGNORECASE)


class MisMatchedArgsError(Exception):
    pass


class NotEnoughArgsError(Exception):
    pass


class NoArgsFoundError(Exception):
    pass


def new_session() -> PromptSession:
    return PromptSession(
        history=InMemoryHistory(),
        auto_suggest=AutoSuggestFromHistory(),
        enable_history_search=True,
    )


def get_key_value_dict(args=List[str]) -> Dict[str, bytes]:
    return {
        args[i]: args[i + 1].encode("utf-8").decode("unicode-escape").encode()
        for i in range(0, len(args), 2)
    }


def execute_get(client: Client, session: PromptSession, args: str) -> None:
    keys = shlex.split(args)
    try:
        key_values = client.get(keys)
        for key, value in key_values.items():
            print_formatted_text(f"  {key} = {value}")
    except Exception as e:
        print_formatted_text(f"Error retrieving values for keys: {e}")


def execute_set(client: Client, session: PromptSession, args: str) -> None:
    keys_and_values = get_key_value_dict(shlex.split(args, posix=True))
    try:
        keys_set = client.set(key_values=keys_and_values)
        print_formatted_text(f"Updated: {', '.join(keys_set)}")
    except Exception as e:
        print_formatted_text(f"Error adding/updating keys: {e}")


def execute_delete(client: Client, session: PromptSession, args: str):
    keys = shlex.split(args)
    try:
        client.delete(keys)
        print_formatted_text(f"Successfully deleted these keys: {keys}")
    except Exception as e:
        print_formatted_text(f"Error deleting keys from store {e}")


def execute_metrics(client: Client, session: PromptSession):
    try:
        metrics = client.metrics()
        print_formatted_text("SERVER METRICS:")
        print_formatted_text(f"  key_count          = {metrics.key_count}")
        print_formatted_text(f"  keys_read_count    = {metrics.keys_read_count}")
        print_formatted_text(f"  keys_updated_count = {metrics.keys_updated_count}")
        print_formatted_text(f"  keys_deleted_count = {metrics.keys_deleted_count}")
        print_formatted_text(f"  total_store_size   = {metrics.total_store_contents_size}")
    except Exception as e:
        print_formatted_text(f"Failed to get metrics from the store: {e}")


def should_continue(session: PromptSession, message: str) -> bool:
    answer = session.prompt(message + " [y/n]: ")
    return answer[0].lower() == "y"


def get_required_args(args: List[str]) -> str:
    if len(args) > 1:
        return args[1]
    raise NoArgsFoundError(
        f"The command '{args[0]}' requires at least one argument.  None were found."
    )


def process_input(ctx, session: PromptSession, input: str, client: Client):
    cmd_and_args = input.split(" ", 1)
    cmd = cmd_and_args[0].strip().upper()
    if cmd == "GET":
        execute_get(client, session, get_required_args(cmd_and_args))
    elif cmd == "SET":
        execute_set(client, session, get_required_args(cmd_and_args))
    elif cmd == "DELETE":
        execute_delete(client, session, get_required_args(cmd_and_args))
    elif cmd == "METRICS":
        execute_metrics(client, session)
    elif cmd in ("QUIT", "Q"):
        if should_continue(session, "Are you sure you want to quit?"):
            exit(0)
    elif cmd == "HELP":
        print_formatted_text(ctx.get_help())
    else:
        if not should_continue(session, "Do you want to continue?"):
            exit(0)


@click.command()
@click.option(
    "--host",
    default="127.0.0.1",
    type=str,
    help="The name of the memkv host you want to connect to"
)
@click.option(
    "--port",
    default=9001,
    type=int,
    help="The port that the memkv server is listening to"
)
@click.option(
    "--debug", is_flag=True, default=False, help="Set this if you want more verbose logging"
)
@click.pass_context
def main(ctx, host: str, port: int, debug: bool):
    """This cli allows one to interact with a memkv server.

    You may interact the server by starting the client and sending commands
    when prompted.  The commands are as follows:\n 
    \b
       GET:
            Retrieves any values stored by the provided keys if any.
            At the prompt type: `GET {key1} {key2} {keyn} ...
            You may set as many keys as you want, they must be separated by spaces.  Any keys
            that have spaces in them, must be surrounded by double quotes to ensure they are
            interpreted correctly.  All keys found in the store will be returned and displayed.
            If a key is not present on return, it means the key doesn't have a value in the
            store.
       SET: 
            Sets the key(s) to the provided value(s)
            At the prompt type: SET {key1} {value1} ... {keyN} {valueN}
            Each token should be space separated with key immediately followed by the value to
            write to that key.  If the key or value has spaces then it must be double quoted so they
            are interpreted correctly.  Note that values are all considered binary representations so
            use the appropriate hex escapes to encode non printable values.
        DELETE:
            Deletes the provided keys from the memory store.
            At the prompt type: DELETE {key1} {key2} ... {key3}
            Each key is delimited by a space. Keys with spaces must be delimited by double quotes.
            If a key is not shown, then the key was not found
        METRICS:
            Returns a list of metrics stored by the server
            At the prompt type: METRICS
            This will return a list of metrics about the server
    """
    print_formatted_text(f"The memkv cli version {version}")
    if debug:
        logger.setLevel("DEBUG")

    session = new_session()
    client = Client(host=host, port=port)
    while True:
        input = session.prompt("> ")
        process_input(ctx, session, input, client)


if __name__ == "__main__":
    main()
