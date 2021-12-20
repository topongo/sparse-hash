#!/usr/bin/env python
import hashlib
import argparse
import os
from sys import stdin
from random import randint as randint
from json import dump as j_dump

units = {"B": 1, "K": 2**10, "M": 2**20, "G": 2**30, "T": 2**40}


def parse_size(size):
    try:
        return int(size)
    except ValueError:
        try:
            if size[-1] in units:
                return int(size[:-1])*units[size[-1]]
            else:
                return False
        except ValueError:
            return False


def hum_size(bytes_):
    if bytes_ < 2**10*.2:
        return f"{bytes_} B"
    elif 2**10*.2 <= bytes_ < 2**20:
        return f"{round(bytes_/2**10, 1)} KB"
    elif 2**20 <= bytes_ < 2**30:
        return f"{round(bytes_/2**20, 1)} MB"
    elif 2**30 <= bytes_ < 2**40:
        return f"{round(bytes_/2**30, 1)} GB"
    elif 2**40 <= bytes_ < 2**50:
        return f"{round(bytes_/2**40, 1)} TB"


def get_file_size(filename):
    fd = os.open(filename, os.O_RDONLY)
    try:
        return os.lseek(fd, 0, os.SEEK_END)
    finally:
        os.close(fd)


arg_parser = argparse.ArgumentParser(prog="sparse-hash", description="Compare partially and at random bytes two files")
arg_parser.add_argument("--percent", help="Percentage of file to scan", type=float)
arg_parser.add_argument("--bytes", help="Number of (kilo|mega|giga|...)bytes to scan.", type=str)
arg_parser.add_argument("--mode", help="Mode to select chunks to scan. "
                                       "Possible options: \"random\", \"duty\", \"chunk\" (default: random)",
                        default="random")
arg_parser.add_argument("--max-chunk-size", help="Maximum buffer stored in memory (per file)", type=str, default=1024*1024)
arg_parser.add_argument("--min-rand-chunk-size", help="Minimum random chunk size to read", type=str, default=1024)
arg_parser.add_argument("--max-rand-chunk-size", help="Maximum random chunk size to read", type=str, default=1024*1024*10)
arg_parser.add_argument("--chunk-start",
                        help="Bytes to skip before hashing, can be used only if --skip-mode is set to \"chunk\" "
                             "(default 0B)", default="0")
arg_parser.add_argument("--duty-chunk-size", help="Set duty chunk size", default=1024*1024)
arg_parser.add_argument("--ignore-size",
                        help="Enable truncated files handling", action="store_true")
arg_parser.add_argument("--dump-scanned-chunks", metavar="DUMP_FILE", help="Dump to specified file the scanned chunk"
                                                                           "positions in json formatting")
arg_parser.add_argument("-q", help="Minimal output, for scripts.", action="store_true")
arg_parser.add_argument("files", metavar="FILE", nargs=2, type=str,
                        help="Files to compare, set one of them as \"-\" to read from stdin.")
arg_parser.add_argument("algorithm", metavar="HASH", nargs="?", type=str, help="Hash algorithm to use (default: md5)",
                        default="md5")
# args = arg_parser.parse_args("--dump-scanned-chunks chunks.json --mode duty --percent 10 1 2".split())
args = arg_parser.parse_args()

if args.max_chunk_size is not None:
    if parse_size(args.max_chunk_size) <= 0:
        arg_parser.error(f"{args.max_chunk_size}: invalid max chunk size")

if args.chunk_start is not None:
    if parse_size(args.chunk_start) < 0:
        arg_parser.error(f"--chunk-start: {args.chunk_start}: invalid size")

to_check = ("max_chunk_size", "max_rand_chunk_size", "min_rand_chunk_size", "duty_chunk_size")
for n in to_check:
    def i():
        return args.__getattribute__(n)
    if i is not None:
        args.__setattr__(n, parse_size(i()))
        if i() <= 0:
            arg_parser.error(f"--{n}: {i()}: invalid size")
            exit(2)

if args.algorithm in hashlib.algorithms_guaranteed:
    dig0 = hashlib.new(args.algorithm)
    dig1 = hashlib.new(args.algorithm)
else:
    arg_parser.error(f"{args.algorithm}: invalid hashing algorithm.")
    exit(3)

if args.files == ["-", "-"]:
    arg_parser.error("only one file can be read from stdin.")

sizes = []
files = []
for a in args.files:
    opener = None
    if a == "-":
        def opener():
            return stdin.buffer
    else:
        if not os.path.exists(a):
            arg_parser.error(f"{a}: no such file or directory")
        elif os.path.isdir(a):
            arg_parser.error(f"{a}: is a directory")
        try:
            with open(a) as f:
                pass
            sizes.append(get_file_size(a))

            def opener():
                return open(a, "rb")
        except IOError:
            arg_parser.error(f"{a}: can't open file")
            exit(2)

    files.append(opener)

f_sizes = 0
if sizes[0] == 0:
    if sizes[1] == 0:
        print("Files are emtpy, so identical.")
        exit()
    elif sizes[1] != 0:
        print("Files differs, one is empty but not the other")
elif sizes[0] != sizes[1]:
    print("Files differs, they've different sizes\n(for truncated files specify --ignore-size and --actual-size")
    exit(3)
else:
    f_size = sizes[0]


if f_size < args.min_rand_chunk_size:
    if not args.q:
        print("sparse-hash: warning: maximum random chunk size is greater than the file size")
    MIN_RAND_CHUNK_SIZE = f_size
else:
    MIN_RAND_CHUNK_SIZE = args.min_rand_chunk_size

amount = 0
if args.percent is not None and args.bytes is not None:
    arg_parser.error("only one of either --percent and --bytes can be specified.")
elif args.percent is not None:
    if args.percent <= 0:
        arg_parser.error(f"{args.percent}: invalid percentage")
    else:
        amount = int(args.percent * f_size / 100)
elif args.bytes is not None:
    amount = parse_size(args.bytes)
    if not amount or amount <= 0:
        arg_parser.error(f"{args.bytes}: invalid number of bytes")
else:
    arg_parser.error("neither --percent or --bytes specified, can't proceed")


def chunks(mode, size, bytes_, chunk_start=None, duty=None):
    read_chunks = []
    if mode == "chunk":
        if not chunk_start:
            yield 0, bytes_
            read_chunks.append((0, bytes_))
        else:
            yield chunk_start, chunk_start+bytes_
    elif mode == "random":
        bytes_read = 0
        _cont = False
        while True:
            if bytes_ == bytes_read:
                break
            s_ = randint(0, size-MIN_RAND_CHUNK_SIZE)
            if bytes_ - bytes_read <= args.max_rand_chunk_size:
                if bytes_ - bytes_read <= MIN_RAND_CHUNK_SIZE:
                    e_ = s_ + bytes_ - bytes_read
                else:
                    e_ = randint(s_+MIN_RAND_CHUNK_SIZE, s_ + bytes_ - bytes_read)
            else:
                e_ = randint(s_+MIN_RAND_CHUNK_SIZE, s_+args.max_rand_chunk_size)
            for s__, e__ in read_chunks:
                if e_ > s__ and s_ < e__:
                    _cont = True
                    break
            if _cont:
                _cont = False
                continue
            else:
                bytes_read += e_-s_
                if e_ < s_:
                    raise
                yield s_, e_-s_
                read_chunks.append((s_, e_))
    elif mode == "duty":
        rem_bytes = bytes_ % duty
        bytes_precise = bytes_ - rem_bytes
        rem_size = size % bytes_precise
        if rem_size <= rem_bytes:
            rem_bytes = rem_size
            if not args.q:
                print(f"\nsparse-hash: warning: the duty of the last cycle has been cut\n")
        size_precise = size - rem_size
        cycles = int(bytes_precise / duty)
        s = 0
        for c in range(cycles):
            s = size_precise / cycles * c
            if s % 1 != 0:
                raise Exception(f"A byte position should be an int (instead it's {s})")
            yield int(s), int(duty)
        yield int(size_precise), int(rem_bytes)
    if args.dump_scanned_chunks:
        j_dump(read_chunks, open(args.dump_scanned_chunks, "w+"), indent=4)


tot_read = 0
f0, f1 = files[0](), files[1]()
try:
    print("Details:\n"
          f"\tFiles to compare: \"{args.files[0]}\", \"{args.files[1]}\"\n"
          f"\tMode: {args.mode}\n"
          f"\tFile size: {hum_size(f_size)}\n"
          f"\tBytes to read: {hum_size(amount)}\n"
          f"\t")

    def read_and_digest(size):
        buff0 = f0.read(size)
        buff1 = f1.read(size)
        dig0.update(buff0)
        dig1.update(buff1)
        if dig0.hexdigest() != dig1.hexdigest():
            print(f"Files differ: different digest at interval of {s}-{s + chunk} bytes")
            exit(0)

    n_chunk = 0

    for s, chunk in chunks(args.mode, f_size, amount, parse_size(args.chunk_start), parse_size(args.duty_chunk_size)):
        f0.seek(s)
        f1.seek(s)
        tot_read += chunk
        n_chunk += 1
        while chunk > args.max_chunk_size:
            read_and_digest(args.max_chunk_size)
            chunk -= args.max_chunk_size
            if not args.q:
                print(f"\rProgress: {round(100.0*tot_read/amount)}% ({tot_read} bytes read, {n_chunk} chunk(s) read)", end="")
        read_and_digest(chunk)
        if not args.q:
            print(f"\rProgress: {round(100.0*tot_read/amount)}% ({tot_read} bytes read, {n_chunk} chunk(s) read)", end="")
    if not args.q:
        print(f"\rProgress: 100% ({tot_read} bytes read)")

    if dig0.hexdigest() == dig1.hexdigest():
        pass
        print(f"Files match certainly at {round(tot_read/f_size*100, 2)}%")
finally:
    f0.close()
    f1.close()
