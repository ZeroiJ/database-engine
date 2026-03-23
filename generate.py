import argparse
import random
import sys


FIRST_NAMES = [
    "Sujal",
    "Alex",
    "Priya",
    "Rahul",
    "Sara",
    "Mike",
    "Ananya",
    "James",
    "Zara",
    "Arjun",
]
LAST_NAMES = [
    "Sharma",
    "Smith",
    "Patel",
    "Johnson",
    "Khan",
    "Lee",
    "Gupta",
    "Brown",
    "Singh",
    "Wilson",
]
CITIES = [
    "Mumbai",
    "Delhi",
    "Bangalore",
    "London",
    "NYC",
    "Tokyo",
    "Berlin",
    "Paris",
    "Dubai",
    "Sydney",
]


def sql_escape_text(s: str) -> str:
    # Your SQL lexer parses strings inside single quotes and terminates at the next `'`.
    # In SQL, escaping is done by doubling quotes: O'Neil -> 'O''Neil'.
    return s.replace("'", "''")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate SQL for 100k rows for the rustdb REPL."
    )
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for repeatable data (optional).",
    )
    parser.add_argument(
        "--no-exit",
        action="store_true",
        help="Do not print `.exit` at the end (useful when writing a .sql file).",
    )
    args = parser.parse_args()

    if args.rows <= 0:
        print("rows must be > 0", file=sys.stderr)
        return 2

    if args.seed is not None:
        random.seed(args.seed)

    print(
        "CREATE TABLE people (id INT, first_name TEXT, last_name TEXT, age INT, city TEXT, salary INT, active BOOLEAN);"
    )

    for i in range(1, args.rows + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        age = random.randint(18, 65)
        city = random.choice(CITIES)
        salary = random.randint(30000, 200000)
        active = "true" if random.random() > 0.3 else "false"

        # One statement per line (the REPL reads line-by-line).
        print(
            f"INSERT INTO people VALUES ({i}, '{sql_escape_text(first)}', '{sql_escape_text(last)}', {age}, '{sql_escape_text(city)}', {salary}, {active});"
        )

    if not args.no_exit:
        print(".exit")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())