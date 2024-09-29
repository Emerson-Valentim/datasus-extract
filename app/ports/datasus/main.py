from pysus.online_data.SIH import download as downloadSIH
from pysus.online_data.SIA import download as downloadSIA
from utils.file_system import FileReference, FileSystem


class _FTPDataSUS:

    def __init__(self, download, groups, dir) -> None:
        self._downloadAdapter = download
        self._groups = groups
        self._dir = dir

    def _download(self, group: str, state: str, year: int, month: int):
        return self._downloadAdapter(state, year, month, groups=[group])

    def _build(
        self, group: str, year: int, month: int, state: str
    ) -> tuple[list[FileReference], str]:
        if state is None or year is None or month is None or group is None:
            raise ValueError("State, year, month and group must be set")

        dir = f"{self._dir}/{group}/{year}/{month}"
        files = self._download(group, state, year, month)
        # This is needed because the download method returns two different interfaces.
        if type(files) != list:
            files = [files]

        frs = []
        for file in files:
            filename = state
            frs.append(
                FileReference(
                    file.to_dataframe(),
                    file.path,
                    dir,
                    filename,
                )
            )

        return frs, dir

    def process(self, group: str, state: str, year: int, month: int):
        [files, dir] = self._build(group, year, month, state)
        for file in files:
            file.persist()
            file.clean()
        fs = FileSystem()
        fs.merge(dir)

    def groups(self):
        return self._groups


class SIH(_FTPDataSUS):
    def __init__(self, dir: str):
        super().__init__(downloadSIH, ["RD"], dir)


class SIA(_FTPDataSUS):
    def __init__(self, dir: str):
        super().__init__(downloadSIA, ["AM", "PA", "PS"], dir)
