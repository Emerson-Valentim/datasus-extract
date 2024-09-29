import shutil
from pathlib import Path
import pandas as pd
import glob
import os


class FileReference:

    def __init__(
        self, dataframe: pd.DataFrame, tempPath: str, folderPath: str, filename: str
    ):
        self._folderPath = folderPath
        self._filename = filename
        self._tempPath = tempPath
        self._dataframe: pd.DataFrame = dataframe

    def persist(self):
        folder = Path(self._folderPath)
        folder.mkdir(parents=True, exist_ok=True)

        self._dataframe.astype(str).to_parquet(
            f"{self._folderPath}/{self._filename}.parquet",
            index=False,
            engine="pyarrow",
        )

    def clean(self):
        shutil.rmtree(self._tempPath)


class FileSystem:
    def merge(self, dir) -> Path:
        mergedFileName = "merged.parquet"
        # Read all Parquet files into a single pandas DataFrame
        all_files = glob.glob(os.path.join(dir, "*.parquet"))
        try:
            all_files.remove(mergedFileName)
        except ValueError:
            pass
        dfs = [
            pd.read_parquet(file, dtype_backend="numpy_nullable") for file in all_files
        ]

        merged_df = pd.concat(dfs, ignore_index=True)

        merged_file = os.path.join(dir, mergedFileName)
        merged_df.to_parquet(merged_file, index=False)

        for file in all_files:
            if file != merged_file:
                os.remove(file)

        return Path(f"{dir}/{mergedFileName}")
