import argparse
import sys
from datetime import datetime, time
from pathlib import Path

# Add the parent directory to the Python path
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent.parent.parent
sys.path.insert(0, str(project_root))

from dc_logging_aws.named_queries.query_template import (  # noqa: E402
    QueryTemplate,
)


class QueryFileCreator:
    def __init__(self, polling_day, start_of_election_period, overwrite=False):
        self.polling_day = polling_day
        self.start_of_election_period = start_of_election_period
        self.overwrite = overwrite
        self.date_str = polling_day.strftime("%Y-%m-%d")
        self.query_template = QueryTemplate(
            polling_day, start_of_election_period
        )
        self.script_dir = Path(__file__).resolve().parent

    @staticmethod
    def valid_date(s):
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except ValueError:
            msg = f"Not a valid date: '{s}'. Please use YYYY-MM-DD format."
            raise argparse.ArgumentTypeError(msg)

    def create_query_directory(self):
        queries_dir = self.script_dir.parent / "queries"
        directory = queries_dir / self.date_str
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    def create_query_files(self):
        directory = self.create_query_directory()

        files_to_create = {
            "election-week-count.sql": self.query_template.postcode_search_count(
                self.query_template.start_of_polling_week
            ),
            "election-day-count.sql": self.query_template.postcode_search_count(
                datetime.combine(self.polling_day, time(0, 0)).replace(
                    tzinfo=self.query_template.close_of_polls.tzinfo
                )
            ),
            "election-period-count.sql": self.query_template.postcode_search_count(
                datetime.combine(
                    self.start_of_election_period, time(0, 0)
                ).replace(tzinfo=self.query_template.close_of_polls.tzinfo)
            ),
            "election-week-postcode-searches-by-product.sql": self.query_template.postcode_searches_by_product(
                self.query_template.start_of_polling_week
            ),
            "election-day-postcode-searches-by-product.sql": self.query_template.postcode_searches_by_product(
                datetime.combine(self.polling_day, time(0, 0)).replace(
                    tzinfo=self.query_template.close_of_polls.tzinfo
                )
            ),
            "election-period-postcode-searches-by-product.sql": self.query_template.postcode_searches_by_product(
                datetime.combine(
                    self.start_of_election_period, time(0, 0)
                ).replace(tzinfo=self.query_template.close_of_polls.tzinfo)
            ),
            "election-week-postcode-searches-by-local-authority.sql": self.query_template.postcode_searches_by_local_authority(
                self.query_template.start_of_polling_week
            ),
            "election-day-postcode-searches-by-local-authority.sql": self.query_template.postcode_searches_by_local_authority(
                datetime.combine(self.polling_day, time(0, 0)).replace(
                    tzinfo=self.query_template.close_of_polls.tzinfo
                )
            ),
            "election-period-postcode-searches-by-local-authority.sql": self.query_template.postcode_searches_by_local_authority(
                datetime.combine(
                    self.start_of_election_period, time(0, 0)
                ).replace(tzinfo=self.query_template.close_of_polls.tzinfo)
            ),
            "election-week-postcode-searches-by-constituency.sql": self.query_template.postcode_searches_by_constituency(
                self.query_template.start_of_polling_week
            ),
            "election-day-postcode-searches-by-constituency.sql": self.query_template.postcode_searches_by_constituency(
                datetime.combine(self.polling_day, time(0, 0)).replace(
                    tzinfo=self.query_template.close_of_polls.tzinfo
                )
            ),
            "election-period-postcode-searches-by-constituency.sql": self.query_template.postcode_searches_by_constituency(
                datetime.combine(
                    self.start_of_election_period, time(0, 0)
                ).replace(tzinfo=self.query_template.close_of_polls.tzinfo)
            ),
        }

        for filename, content in files_to_create.items():
            file_path = directory / filename
            if file_path.exists() and not self.overwrite:
                print(
                    f"File {file_path} already exists. Use --overwrite to replace existing files."
                )
            else:
                with open(file_path, "w") as f:
                    f.write(content)
                print(f"Created {filename} in {directory}")

        print(f"Process completed for {self.date_str}")

    @classmethod
    def handle(cls):
        parser = argparse.ArgumentParser(
            description="Create election query files for a specific polling day."
        )
        parser.add_argument(
            "polling_day",
            type=cls.valid_date,
            help="The polling day in YYYY-MM-DD format",
        )
        parser.add_argument(
            "start_of_election_period",
            type=cls.valid_date,
            help="The start date of the election period in YYYY-MM-DD format",
        )
        parser.add_argument(
            "-o",
            "--overwrite",
            action="store_true",
            help="Overwrite existing files",
        )

        args = parser.parse_args()

        creator = cls(
            args.polling_day, args.start_of_election_period, args.overwrite
        )
        creator.create_query_files()


if __name__ == "__main__":
    QueryFileCreator.handle()
