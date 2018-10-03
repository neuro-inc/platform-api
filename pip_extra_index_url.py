from pip import create_main_parser


def main():
    parser = create_main_parser()
    parser.config.read(parser.files)
    try:
        urls = parser.config["global"]["extra-index-url"].splitlines()
        print(", ".join(urls))
    except Exception:
        pass


if __name__ == "__main__":
    main()
