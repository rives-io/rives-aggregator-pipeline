import json

from dagster import ConfigurableResource
from upath import UPath


class PublicBucket(ConfigurableResource):
    base_path: str = './data'

    def write_tournament_leaderboard(
        self,
        data: dict,
        tournament_name: str
    ) -> str:
        """Write the leaderboard file and return the final path"""
        base = UPath(self.base_path)

        output_dir = base / 'tournament' / tournament_name
        output_dir.mkdir(exist_ok=True, parents=True)

        output_file = output_dir / 'leaderboard.json'

        with output_file.open('wt') as fout:
            json.dump(data, fout)

        return str(output_file)
