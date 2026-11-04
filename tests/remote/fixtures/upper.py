"""Write an upper-cased copy of a file, for the remote script task tests."""

import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--src")
parser.add_argument("--dest")
args = parser.parse_args()
Path(args.dest).write_text(Path(args.src).read_text().upper())
