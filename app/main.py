from utils.states import UFs
from ports.datasus.main import SIH, SIA


def downloadData(
    initializer=None, ufs=[], currentTry=0, processedGroups=[], instance=None
):
    # Try to download the data 3 times.
    if currentTry > 3:
        return

    # If no UFs are passed, download all of them.
    if len(ufs) == 0:
        ufs = UFs[:]

    # If no instance is passed, create a new one.
    if instance is None:
        instance = initializer(f"/data")

    try:
        year = 2020
        for group in instance.groups():
            # Skip already processed groups.
            if group in processedGroups:
                continue

            for month in range(1, 13):
                while len(ufs) > 0:
                    uf = ufs.pop()
                    fs = instance.process(group, uf, year, month)
                fs.merge()
                # If the download is successful, reset UFs.
                ufs = UFs[:]
                break

            # Add the processed group to the list.
            processedGroups.append(group)
    except Exception as e:
        print(f"Failed during download {e}")
    finally:
        # Try to download the data again.
        downloadData(
            ufs=ufs,
            currentTry=currentTry + 1,
            processedGroups=processedGroups,
            instance=instance,
        )


def main():
    downloadData(SIA)


main()
