from configparser import ConfigParser


def config(filename: str, section="postgresql"):
    """
    Reads database configurations from file
    :param filename:
    :param section:
    :return: dictionary with the config data
    """
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
    config_db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config_db[param[0]] = param[1]
    else:
        raise Exception(
            f"Section {section} is not found in the {filename} file.")
    return config_db
