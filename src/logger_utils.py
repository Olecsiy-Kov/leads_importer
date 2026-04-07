VERBOSE = False
DEBUG = False


def info(message: str):
    print(message)


def warn(message: str):
    print(f"[WARN] {message}")


def error(message: str):
    print(f"[ERROR] {message}")


def verbose(message: str):
    if VERBOSE:
        print(message)


def debug(message: str):
    if DEBUG:
        print(message)