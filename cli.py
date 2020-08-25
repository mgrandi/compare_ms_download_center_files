 #!/usr/bin/env python3

# library imports
import argparse
import logging
import sys
import pathlib

# third party imports
import arrow
import attr
import logging_tree


from compare_ms_download_center_files import app

class ArrowLoggingFormatter(logging.Formatter):
    ''' logging.Formatter subclass that uses arrow, that formats the timestamp
    to the local timezone (but its in ISO format)
    '''

    def formatTime(self, record, datefmt=None):
        return arrow.get("{}".format(record.created), "X").to("local").isoformat()

def isFolderType(filePath):
    ''' see if the file path given to us by argparse is a file
    @param filePath - the filepath we get from argparse
    @return the filepath as a pathlib.Path() if it is a file, else we raise a ArgumentTypeError'''

    path_maybe = pathlib.Path(filePath)
    path_resolved = None

    # try and resolve the path
    try:
        path_resolved = path_maybe.expanduser().resolve(strict=True)

    except Exception as e:
        raise argparse.ArgumentTypeError("Failed to parse `{}` as a path: `{}`".format(filePath, e))

    # double check to see if its a file
    if not path_resolved.is_dir():
        raise argparse.ArgumentTypeError("The path `{}` is not a file!".format(path_resolved))

    return path_resolved


if __name__ == "__main__":
    # if we are being run as a real program

    parser = argparse.ArgumentParser(
        description="compares the microsoft download center files that I downloaded from the ones everyone else downloaded",
        epilog="Copyright 2020-08-15 - Mark Grandi")

    # set up logging stuff
    logging.captureWarnings(True) # capture warnings with the logging infrastructure
    root_logger = logging.getLogger()
    logging_formatter = ArrowLoggingFormatter("%(asctime)s %(threadName)-10s %(name)-20s %(levelname)-8s: %(message)s")
    logging_handler = logging.StreamHandler(sys.stdout)
    logging_handler.setFormatter(logging_formatter)
    root_logger.addHandler(logging_handler)


    parser.add_argument('--warc-file-folder', dest="warc_file_folder", required=True, type=isFolderType, help="the WARC folder")
    parser.add_argument('--cdx-file-folder', dest="cdx_file_folder", required=True, type=isFolderType, help="the CDX folder")

    parser.add_argument("--verbose", action="store_true", help="Increase logging verbosity")


    try:
        parsed_args = parser.parse_args()

        # set logging level based on arguments
        if parsed_args.verbose:
            root_logger.setLevel("DEBUG")
        else:
            root_logger.setLevel("INFO")

        root_logger.debug("Parsed arguments: %s", parsed_args)
        root_logger.debug("Logger hierarchy:\n%s", logging_tree.format.build_description(node=None))

        # run the application
        app = app.Application(root_logger.getChild("app"), parsed_args)
        app.run()

        root_logger.info("Done!")
    except Exception as e:
        root_logger.exception("Something went wrong!")
        sys.exit(1)