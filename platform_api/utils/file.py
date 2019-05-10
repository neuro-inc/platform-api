def read_file(file: str) -> str:
    with open(file, "r") as f:
        return f.read()
