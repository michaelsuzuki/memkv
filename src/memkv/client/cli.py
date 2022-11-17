import logging
import re
import shlex
from typing import Callable, Dict, Final, List, Union, Type

import click
from prompt_toolkit import PromptSession, print_formatted_text
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.history import InMemoryHistory

from memkv.client.api import Client

PARSED_ARGS_T: Final[Type] = Union[List[str], Dict[str, bytes]]

version: Final[str] = "0.1"

class ParseArgsError(Exception):
    pass


log_format: Final[str] = \
    "%(asctime)s::%(levelname)s::%(name)s::%(filename)s::%(lineno)d::%(message)s"

logging.basicConfig(
    level="INFO",
    format=log_format
)
logger: Final[str] = logging.getLogger(__name__)

cmd_pat: Final[re.Pattern] = re.compile(r"^\s*(GET|SET|DELETE|METRICS)\s*", re.IGNORECASE)


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
    try:
        return {
            args[i]: args[i + 1].encode("utf-8").decode("unicode-escape").encode()
            for i in range(0, len(args), 2)
        }
    except IndexError:
        raise ParseArgsError("Unable to parse arguments: Mismatched keys and/or values")


def execute_get(client: Client, keys: List[str]) -> None:
    try:
        key_values = client.get(keys)
        for key, value in key_values.items():
            print_formatted_text(f"  {key} = {value}")
    except Exception as e:
        print_formatted_text(f"Error retrieving values for keys: {e.__class__.__name__}")


def parse_args_string(
    client: Client, args_str: str, parse_fn: Callable[[str], PARSED_ARGS_T]
) -> PARSED_ARGS_T:
    return parse_fn(args_str)


def parse_keys(args_str: str) -> List[str]:
    return shlex.split(args_str)


def parse_key_values(args_str: str) -> Dict[str, bytes]:
    return get_key_value_dict(shlex.split(args_str, posix=True))


def execute_set(client: Client, key_values: Dict[str, bytes]) -> None:
    try:
        keys_set = client.set(key_values=key_values)
        print_formatted_text(f"Updated: {', '.join(keys_set)}")
    except Exception as e:
        print_formatted_text(f"Error adding/updating keys: {e}")


def execute_delete(client: Client, keys: List[str]):
    try:
        client.delete(keys)
        print_formatted_text(f"Successfully deleted these keys: {keys}")
    except Exception as e:
        print_formatted_text(f"Error deleting keys from store {e}")


def execute_metrics(client: Client):
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


def get_args_string(args: List[str]) -> str:
    if len(args) > 1:
        return args[1]
    raise NoArgsFoundError(
        f"The command '{args[0]}' requires at least one argument.  None were found."
    )


def process_input(ctx, session: PromptSession, input: str, client: Client):
    cmd_and_args = input.split(" ", 1)
    cmd = cmd_and_args[0].strip().upper()
    if cmd == "GET":
        execute_get(client, parse_args_string(get_args_string(cmd_and_args), parse_keys))
    elif cmd == "SET":
        try:
            execute_set(client, parse_args_string(get_args_string(cmd_and_args), parse_key_values))
        except ParseArgsError as e:
            print_formatted_text(f"Error for cmd {cmd}: {e}")
    elif cmd == "DELETE":
        execute_delete(client, parse_args_string(get_args_string(cmd_and_args), parse_keys))
    elif cmd == "METRICS":
        execute_metrics(client)
    elif cmd in ("QUIT", "Q"):
        if should_continue(session, "Are you sure you want to quit?"):
            exit(0)
    elif cmd == "HELP":
        print_formatted_text(ctx.get_help())
    else:
        if cmd:
            print_formatted_text(
                f"Unknown command entered: {cmd}. \
                    You can enter 'help' for more information or try again"
            )


def process_cmd_with_args(client: Client, cmd: str, args: PARSED_ARGS_T):
    if cmd.upper() == "GET":
        execute_get(client, args)
    elif cmd.upper() == "SET":
        try:
            execute_set(client, get_key_value_dict(args))
        except ParseArgsError as e:
            print_formatted_text(f"Error executing command {cmd}: {e}")
    elif cmd.upper() == "DELETE":
        execute_delete(client, args)
    elif cmd.upper() == "METRICS":
        execute_metrics(client)
    else:
        print_formatted_text(f"Error: Unable to execute unknown command {cmd}")
        exit(-1)
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
@click.argument(
    "cmd", nargs=1, type=click.Choice(["GET", "SET", "DELETE", "METRICS"], case_sensitive=False)
)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@click.pass_context
def main(ctx, host: str, port: int, cmd: str, args: List[Union[str, bytes]], debug: bool):
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

    client = Client(host=host, port=port)
    if cmd is not None:
        print(f"Running command: {cmd}")
        process_cmd_with_args(client, cmd, args)

    session = new_session()
    while True:
        try:
            input = session.prompt("> ")
            process_input(ctx, session, input, client)
        except Exception as e:
            print_formatted_text(f"Error executing command: {e}")


if __name__ == "__main__":
    main()
