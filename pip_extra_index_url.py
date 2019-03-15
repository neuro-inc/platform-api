from pip._internal.configuration import Configuration


def main() -> None:
    config = Configuration(isolated=False)
    config.load()
    try:
        urls = config.get_value("global.extra-index-url").splitlines()
        print(", ".join(urls))
    except Exception:
        pass


if __name__ == "__main__":
    main()
