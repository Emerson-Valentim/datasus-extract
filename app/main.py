from utils.states import UFs
from ports.datasus.main import SIH, SIA


def downloadData(initializer):
    instance = initializer(f"/data")
    year = 2020
    for group in instance.groups():
        for month in range(1, 13):
            for UF in UFs:
                instance.process(group, UF, year, month)
            break


def main():
    downloadData(SIH)


main()
