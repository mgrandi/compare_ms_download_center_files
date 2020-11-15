import logging
import pathlib
import gzip
import urllib.parse
import pprint
import json

import attr
import arrow
import warcio
from warcio.archiveiterator import ArchiveIterator

@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class Entry:
    full_url:str = attr.ib()
    file_hash:str = attr.ib()
    entry_type:str = attr.ib(eq=False)
    found_in_file:str = attr.ib(eq=False)



class Application:
    ''' the main application
    '''

    def __init__(self, logger, args):
        ''' constructor
        @param logger the Logger instance
        @param args - the namespace object we get from argparse.parse_args()
        '''

        self.logger = logger
        self.args = args

        self.warc_entries_list = list()
        self.cdx_entries_list = list()
        self.warc_error_list = list()

    def run(self):




        # go through the CDX folder

        for iter_cdx_gz_path in self.args.cdx_file_folder.rglob("*.cdx.gz"):

            self.logger.info("on CDX file: `%s`", iter_cdx_gz_path)

            cnt = 0
            with gzip.open(iter_cdx_gz_path, "rt", encoding="utf-8") as gzip_file:

                first_line_found = False

                while True:

                    line = gzip_file.readline()

                    # EOF
                    if not line:
                        break

                    # skip first line
                    if not first_line_found:
                        first_line_found = True
                        continue


                    #https://iipc.github.io/warc-specifications/specifications/cdx-format/cdx-2015/
                    # the format of the CDX files are:
                    # `N b a m s k r M S V g`
                    #  `k` for `new style checksum `
                    # `a` for `original url`
                    parts = line.split(" ")

                    new_style_checksum = parts[5]
                    full_url =  urllib.parse.unquote(parts[2])

                    new_cdx_entry = Entry(
                        full_url=full_url,
                        file_hash=new_style_checksum,
                        entry_type="CDX",
                        found_in_file=str(iter_cdx_gz_path))

                    self.cdx_entries_list.append(new_cdx_entry)

                    self.logger.debug("CDX entry: `%s`", new_cdx_entry)

                    cnt += 1

            self.logger.info("loaded `%s` from CDX file: `%s`", cnt, iter_cdx_gz_path)


        self.logger.info("loaded `%s` total CDX entries", len(self.cdx_entries_list))


        # go through warc folders

        for iter_warc_gz_path in self.args.warc_file_folder.rglob("*warc.gz"):

            self.logger.info("on WARC file `%s`", iter_warc_gz_path)

            try:
                cnt = 0
                with open(iter_warc_gz_path, 'rb') as stream:

                    for record in ArchiveIterator(stream):
                        if record.rec_type == 'response':
                            # import pdb;pdb.set_trace()

                            file_hash = record.rec_headers.get_header('WARC-Payload-Digest')

                            # the hashes are actually like this:
                            # `sha1:EGD47VYGZKWHT6PTS7HJN7D4TCEKSWVC`, so we need to strip off the `sha1:` part
                            sha_str_prefix = "sha1:"

                            if file_hash.startswith(sha_str_prefix):
                                file_hash = file_hash[len(sha_str_prefix):]

                            full_url = urllib.parse.unquote(record.rec_headers.get_header("WARC-Target-URI"))

                            new_warc_entry = Entry(
                                full_url=full_url,
                                file_hash=file_hash,
                                entry_type="WARC",
                                found_in_file=str(iter_warc_gz_path))

                            self.logger.debug("WARC entry: `%s`", new_warc_entry)

                            self.warc_entries_list.append(new_warc_entry)
                            cnt += 1

                self.logger.info("loaded `%s` from WARC file: `%s`", cnt, iter_warc_gz_path)


            except warcio.exceptions.ArchiveLoadFailed as e:
                self.logger.error("WARC file failed to load: `%s`, skipping", iter_warc_gz_path)
                self.warc_error_list.append(iter_warc_gz_path)
                continue

        self.logger.info("loaded `%s` total WARC entries", len(self.warc_entries_list))


        # in reverse order from what i think it should be, its warc.difference(cdx) instead of the other way around
        #
        # >>> x
        # [1, 2, 3]
        # >>> y
        # [2, 3, 4, 5, 6, 7, 8]
        # >>> set(x).difference(set(y))
        # {1}
        # >>> set(y).difference(set(x))
        # {4, 5, 6, 7, 8}
        #
        set_of_cdx_not_in_warc = set(self.warc_entries_list).difference(set(self.cdx_entries_list))

        self.logger.info("`%s` CDX entries were found that were not in the WARC entries set", len(set_of_cdx_not_in_warc))

        self.logger.info("%s", pprint.pformat(set_of_cdx_not_in_warc))

        iso_str = str(arrow.utcnow().timestamp)

        output_folder_path = self.args.output_folder / f"{iso_str}_ms_dl_comp_results"

        output_folder_path.mkdir()

        warc_file_path = output_folder_path / f"{iso_str}_warc_entries.txt"
        cdx_file_path = output_folder_path / f"{iso_str}_cdx_entries.txt"
        differences_path = output_folder_path / f"{iso_str}_differences.txt"

        warc_error_path = output_folder_path / f"{iso_str}_warc_errors.txt"

        warc_files_to_save_path = output_folder_path / f"{iso_str}_warc_files_to_save.txt"


        self.logger.info("writing WARC entries to `%s`", warc_file_path)
        with open(warc_file_path, "w", encoding="utf-8") as f:

            for iter_p in self.warc_entries_list:

                f.write(json.dumps(attr.asdict(iter_p), indent=4))
                f.write("\n\n")


        self.logger.info("writing CDX entries to `%s`", cdx_file_path)
        with open(cdx_file_path, "w", encoding="utf-8") as f:

            for iter_p in self.cdx_entries_list:

                f.write(json.dumps(attr.asdict(iter_p), indent=4))
                f.write("\n\n")


        self.logger.info("writing differences to `%s`", differences_path)
        with open(differences_path, "w", encoding="utf-8") as f:

            for iter_p in set_of_cdx_not_in_warc:

                f.write(json.dumps(attr.asdict(iter_p), indent=4))
                f.write("\n\n")


        self.logger.info("writing warc errors to `%s`", warc_error_path)
        with open(warc_error_path, "w", encoding="utf-8") as f:
            for iter_p in self.warc_error_list:
                f.write(str(iter_p))
                f.write("\n")


        self.logger.info("writing warc files to save file to `%s`", warc_files_to_save_path)
        with open(warc_files_to_save_path, "w", encoding="utf-8") as f:

            # save the warc files with errors
            for iter_p in self.warc_error_list:
                f.write(str(iter_p))
                f.write("\n")

            # save the warc files that we have unique files for
            # but deduplicate the entries so we don't add the same file multiple times for readability
            set_of_warc_paths = set()
            for iter_e in set_of_cdx_not_in_warc:
                set_of_warc_paths.add(iter_e.found_in_file)

            for iter_x in set_of_warc_paths:
                f.write(iter_x)
                f.write("\n")

