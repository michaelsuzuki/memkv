import re
import shlex
from typing import Dict, List

from prompt_toolkit import PromptSession, print_formatted_text
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.history import InMemoryHistory

version = "0.1"

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
        args[i]: args[i + 1].encode("utf-8").decode("unicode-escape")
        for i in range(0, len(args), 2)
    }


def execute_get(session: PromptSession, args: str):
    keys = shlex.split(args)
    print(f"KEYS: {keys}")


def execute_set(session: PromptSession, args: str):
    keys_and_values = get_key_value_dict(shlex.split(args, posix=True))
    print(f"Keys and Values: {keys_and_values}")


def execute_delete(session: PromptSession, args: str):
    keys = shlex.split(args)
    print(f"Keys = {keys}")


def should_continue(session: PromptSession, message: str) -> bool:
    answer = session.prompt(message + " [y|n]: ")
    return answer[0].lower() == "y"


def get_required_args(session, command: str, args: List[str]) -> str:
    if len(args) > 1:
        return args[1]
    raise NoArgsFoundError(
        f"The command '{command}' requires at least one argument.  None were found."
    )


def process_input(session: PromptSession, input: str):
    cmd_and_args = input.split(" ", 1)
    cmd = cmd_and_args[0].strip().upper()
    if cmd == "GET":
        execute_get(session, get_required_args(cmd_and_args))
    elif cmd == "SET":
        execute_set(session, get_required_args(cmd_and_args))
    elif cmd == "DELETE":
        execute_delete(session, get_required_args(cmd_and_args))
    elif cmd == "METRICS":
        pass
    elif cmd in ("QUIT", "q"):
        if should_continue(session, "Are you sure you want to quit?"):
            exit(0)
    else:
        if not should_continue(session, "Do you want to continue?"):
            exit(0)


def main():
    print_formatted_text(f"The memkv cli version {version}")
    session = new_session()
    input = session.prompt("> ")
    while True:
        process_input(session, input)


if __name__ == "__main__":
    main()
